// main is the entrypoint for the ctile binary.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fxamacker/cbor/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"
)

// parseQueryParams returns the start and end values, or an error.
//
// The end value it returns is one greater than in the request,
// because CT uses closed intervals while we use half-open intervals
// internally for simpler math.
func parseQueryParams(values url.Values) (int64, int64, error) {
	start := values.Get("start")
	end := values.Get("end")
	if start == "" {
		return 0, 0, errors.New("missing start parameter")
	}
	if end == "" {
		return 0, 0, errors.New("missing end parameter")
	}
	startInt, err := strconv.ParseInt(start, 10, 64)
	if err != nil || startInt < 0 {
		return 0, 0, fmt.Errorf("invalid start parameter: %w", err)
	}
	endInt, err := strconv.ParseInt(end, 10, 64)
	if err != nil || endInt < 0 {
		return 0, 0, fmt.Errorf("invalid end parameter: %w", err)
	}
	if endInt < startInt {
		return 0, 0, errors.New("end must be greater than or equal to start")
	}
	return startInt, endInt + 1, nil
}

// tile represents important info about a tile: where it starts, where it ends, its size,
// what CT backend URL it exists on (or is anticipated to exist on), and what s3 prefix
// it should be stored/retrieved under.
//
// `start` is inclusive, and `end` is exclusive, unlike in the CT protocol.
// In other words, they represent the half-open interval [start, end).
type tile struct {
	start  int64
	end    int64
	size   int64
	logURL string
}

// makeTile returns a tile of size `size` that contains the given `start` position.
// The resulting tile's `start` will be equal to or less than the requested `start`.
func makeTile(start, size int64, logURL string) tile {
	tileOffset := start % size
	tileStart := start - tileOffset
	return tile{
		start:  tileStart,
		end:    tileStart + size,
		size:   size,
		logURL: logURL,
	}
}

// key returns the S3 key for the tile.
func (t tile) key() string {
	return fmt.Sprintf("tile_size=%d/%d.cbor.gz", t.size, t.start)
}

// url returns the URL to fetch the tile from the backend.
func (t tile) url() string {
	// Use end-1 because our internal representation uses half-open intervals, while the
	// CT protocol uses closed intervals. https://datatracker.ietf.org/doc/html/rfc6962#section-4.6
	return fmt.Sprintf("%s/ct/v1/get-entries?start=%d&end=%d", t.logURL, t.start, t.end-1)
}

// entries corresponds to the JSON response to the CT get-entries endpoint.
// https://datatracker.ietf.org/doc/html/rfc6962#section-4.6
//
// It is marshaled and unmarshaled to/from JSON and CBOR.
//
// This type must not be mutated, because pointers to the same value may be in use
// across multiple goroutines.
type entries struct {
	Entries []entry `json:"entries"`
}

type pastTheEndError struct{}

func (p pastTheEndError) Error() string {
	return "requested range is past the end of the log"
}

// trimForDisplay takes a set of entries corresponding to `tile`, and returns a new
// object suitable for a request for entries in the range [start, end).
//
// This does not mutate the original object. It is suitable for calling when the set
// of entries represents a partial tile.
func (e *entries) trimForDisplay(start, end int64, tile tile) (*entries, error) {
	if start < tile.start || start >= tile.end || end <= start || len(e.Entries) > int(tile.size) {
		return nil, fmt.Errorf("internal inconsistency: start = %d, end = %d, tile = %v, len(e.Entries) = %d", start, end, tile, len(e.Entries))
	}

	// Truncate to match the request
	prefixToRemove := start - tile.start
	if prefixToRemove >= int64(len(e.Entries)) {
		// In this case, the requested range is entirely outside the current log,
		// but the _tile_'s beginning was inside the log. For instance, a log with
		// size 1000 and max_getentries of 256, where ctile is handling a request
		// for start=1001&end=1001; the tile starts at offset 768, but is partial so
		// it doesn't include the requested range.
		//
		// When Trillian gets a request that is past the end of the log, it returns
		// 400 (for better or worse), so we emulate that here.
		return nil, pastTheEndError{}
	}

	requestedLen := end - start
	if prefixToRemove+requestedLen > int64(len(e.Entries)) {
		requestedLen = int64(len(e.Entries)) - prefixToRemove
	}
	return &entries{
		Entries: e.Entries[prefixToRemove : prefixToRemove+requestedLen],
	}, nil
}

// entry corresponds to a single entry in the CT get-entries endpoint.
//
// Note: the JSON fields are base64. For fields of type `[]byte`, Go's encoding/json
// automagically decodes base64.
//
// This type must not be mutated, because pointers to the same value may be in use
// across multiple goroutines.
type entry struct {
	LeafInput []byte `json:"leaf_input"`
	ExtraData []byte `json:"extra_data"`
}

// statusCodeError indicates the backend returned a non-200 status code, and contains
// the response body. This allows passing through that status code and body to the requester.
type statusCodeError struct {
	statusCode int
	body       []byte
}

func (s statusCodeError) Error() string {
	return fmt.Sprintf("backend responded with status code %d and body:\n%s", s.statusCode, string(s.body))
}

// getTileFromBackend fetches a tile of entries from the backend.
//
// If the backend returns a non-200 status code, it returns a statusCodeError,
// so the caller can handle that case specially by propagating the backend's
// status code (for instance, 400 or 404).
func getTileFromBackend(ctx context.Context, t tile) (*entries, error) {
	url := t.url()
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create backend Request object: %w", err)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading body from %s: %w", url, err)
		}
		return nil, statusCodeError{resp.StatusCode, body}
	}

	var entries entries
	err = json.NewDecoder(resp.Body).Decode(&entries)
	if err != nil {
		return nil, fmt.Errorf("reading body from %s: %w", url, err)
	}

	if len(entries.Entries) > int(t.size) || len(entries.Entries) == 0 {
		return nil, fmt.Errorf("expected %d entries, got %d", t.size, len(entries.Entries))
	}

	return &entries, nil
}

// writeToS3 stores the entries corresponding to the given tile in s3.
func (tch *tileCachingHandler) writeToS3(ctx context.Context, t tile, e *entries) error {
	if len(e.Entries) != int(t.size) || t.end != t.start+t.size {
		return fmt.Errorf("internal inconsistency: len(entries) == %d; tile = %v", len(e.Entries), t)
	}

	var body bytes.Buffer
	w := gzip.NewWriter(&body)
	err := cbor.NewEncoder(w).Encode(e)
	if err != nil {
		return nil
	}

	err = w.Close()
	if err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	}

	key := tch.s3Prefix + t.key()
	_, err = tch.s3Service.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(tch.s3Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("putting in bucket %q with key %q: %s", tch.s3Bucket, key, err)
	}
	return nil
}

// noSuchKey indicates the requested key does not exist.
type noSuchKey struct{}

func (noSuchKey) Error() string {
	return "no such key"
}

// getFromS3 retrieves the entries corresponding to the given tile from s3.
// If the tile isn't already stored in s3, it returns a noSuchKey error.
func (tch *tileCachingHandler) getFromS3(ctx context.Context, t tile) (*entries, error) {
	key := tch.s3Prefix + t.key()
	resp, err := tch.s3Service.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(tch.s3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, noSuchKey{}
		}
		return nil, fmt.Errorf("getting from bucket %q with key %q: %w", tch.s3Bucket, key, err)
	}

	var entries entries
	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("making gzipReader: %w", err)
	}
	err = cbor.NewDecoder(gzipReader).Decode(&entries)
	if err != nil {
		return nil, fmt.Errorf("reading body from bucket %q with key %q: %w", tch.s3Bucket, key, err)
	}

	if len(entries.Entries) != int(t.size) || t.end != t.start+t.size {
		return nil, fmt.Errorf("internal inconsistency: len(entries) == %d; tile = %v", len(entries.Entries), t)
	}

	return &entries, nil
}

// tileCachingHandler is the main HTTP handler that serves CT tiles it fetches
// from a backend server and from the cache tiles it maintains in S3.
type tileCachingHandler struct {
	logURL   string // The string form of the HTTP host and path prefix to add incoming request paths to in order to fetch tiles from the backing CT log. Must not be empty.
	tileSize int    // The CT tile size used here and in the backing CT log. Must be the same as the backing CT log's value and must not be zero.

	s3Service *s3.Client // The S3 service to use for caching tiles. Must not be nil.
	s3Prefix  string     // The prefix to add to the path when caching tiles in S3. Must not be empty.
	s3Bucket  string     // The S3 bucket to use for caching tiles. Must not be empty.

	cacheGroup *singleflight.Group // The singleflight.Group to use for deduplicating simultaneous requests (a.k.a. "request collapsing") for tiles. Must not be nil.

	requestsMetric       *prometheus.CounterVec
	partialTiles         prometheus.Counter
	singleFlightShared   prometheus.Counter
	latencyMetric        prometheus.Histogram
	backendLatencyMetric *prometheus.HistogramVec

	fullRequestTimeout time.Duration
}

func newTileCachingHandler(
	logURL string,
	tileSize int,
	s3Service *s3.Client,
	s3Prefix string,
	s3Bucket string,
	fullRequestTimeout time.Duration,
	promRegisterer prometheus.Registerer,
) (*tileCachingHandler, error) {
	if logURL == "" {
		return nil, errors.New("logURL must not be empty")
	}
	if tileSize == 0 {
		return nil, errors.New("tileSize must not be zero")
	}
	if s3Service == nil {
		return nil, errors.New("s3Service must not be nil")
	}
	if s3Prefix == "" {
		return nil, errors.New("s3Prefix must not be empty")
	}
	if s3Bucket == "" {
		return nil, errors.New("s3Bucket must not be empty")
	}
	if fullRequestTimeout == 0 {
		return nil, errors.New("fullRequestTimeout must not be zero")
	}
	requestsMetric := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ctile_requests",
			Help: "total number of requests, by result and source",
		},
		[]string{"result", "source"},
	)
	promRegisterer.MustRegister(requestsMetric)

	partialTiles := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ctile_partial_tiles",
			Help: "number of requests not cached due to partial tile returned from CT log",
		})
	promRegisterer.MustRegister(partialTiles)

	singleFlightShared := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "ctile_single_flight_shared",
			Help: "number of inbound requests coalesced into a single set of backend requests",
		})
	promRegisterer.MustRegister(singleFlightShared)

	latencyMetric := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ctile_response_latency_seconds",
			Help:    "overall latency of responses, including all backend requests",
			Buckets: prometheus.DefBuckets,
		})
	promRegisterer.MustRegister(latencyMetric)

	backendLatencyMetric := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ctile_backend_latency_seconds",
			Help:    "latency of each backend request",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"backend"})
	promRegisterer.MustRegister(backendLatencyMetric)

	return &tileCachingHandler{
		logURL:               logURL,
		tileSize:             tileSize,
		s3Service:            s3Service,
		s3Prefix:             s3Prefix,
		s3Bucket:             s3Bucket,
		cacheGroup:           &singleflight.Group{},
		requestsMetric:       requestsMetric,
		partialTiles:         partialTiles,
		singleFlightShared:   singleFlightShared,
		fullRequestTimeout:   fullRequestTimeout,
		latencyMetric:        latencyMetric,
		backendLatencyMetric: backendLatencyMetric,
	}, nil
}

func (tch *tileCachingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()
	defer func() {
		tch.latencyMetric.Observe(time.Since(begin).Seconds())
	}()

	if !strings.HasSuffix(r.URL.Path, "/ct/v1/get-entries") {
		passthroughHandler{logURL: tch.logURL}.ServeHTTP(w, r)
		return
	}
	start, end, err := parseQueryParams(r.URL.Query())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), tch.fullRequestTimeout)
	defer cancel()

	tile := makeTile(start, int64(tch.tileSize), tch.logURL)

	contents, source, err := tch.getAndCacheTile(ctx, tile)
	if err != nil {
		status := http.StatusInternalServerError
		var statusCodeErr statusCodeError
		if errors.As(err, &statusCodeErr) {
			status = statusCodeErr.statusCode
		}
		// Send errors to our stdout as well as to the user.
		if status != http.StatusBadRequest {
			log.Println(err)
		}
		w.WriteHeader(status)
		fmt.Fprintln(w, err)
		return
	}

	if tch.isPartialTile(contents) {
		w.Header().Set("X-Partial-Tile", "true")
	}

	w.Header().Set("X-Source", string(source))

	contents, err = contents.trimForDisplay(start, end, tile)
	if err != nil {
		if errors.As(err, &pastTheEndError{}) {
			tch.requestsMetric.WithLabelValues("bad_request", "past_the_end_partial_tile").Inc()
		} else {
			tch.requestsMetric.WithLabelValues("error", "internal_inconsistency").Inc()
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, err)
		return
	}

	if w.Header().Get("X-Source") == "S3" {
		tch.requestsMetric.WithLabelValues("success", "s3_get").Inc()
	} else {
		tch.requestsMetric.WithLabelValues("success", "ct_log_get").Inc()
	}

	w.Header().Set("X-Response-Len", fmt.Sprintf("%d", len(contents.Entries)))
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.Encode(contents)
}

// tileSource is a helper enum to indicate to the user whether the tile returned
// to them was found in S3 or in the CT log.
type tileSource string

const (
	sourceCTLog tileSource = "CT log"
	sourceS3    tileSource = "S3"
)

// getAndCacheTile fetches the requested tile from S3 if it exists there, or, if
// it doesn't exist in S3, from the backing CT log and then caches it in S3.
// Under the hood, it collapses requests for the same tile into one single
// request. It should be preferred over getAndCacheTileUncollapsed.
func (tch *tileCachingHandler) getAndCacheTile(ctx context.Context, tile tile) (*entries, tileSource, error) {
	dedupKey := fmt.Sprintf("logURL-%s-tile-%d-%d", tile.logURL, tile.start, tile.end)

	type entriesAndSource struct {
		entries *entries
		source  tileSource
	}

	innerContents, err, shared := singleflightDo(tch.cacheGroup, dedupKey, func() (entriesAndSource, error) {
		contents, source, err := tch.getAndCacheTileUncollapsed(ctx, tile)
		return entriesAndSource{contents, source}, err
	})

	if shared {
		tch.singleFlightShared.Inc()
	}

	// The value from our singleflightDo closure is always non-nil, so we don't
	// need an err != nil check here.
	return innerContents.entries, innerContents.source, err
}

// getAndCacheTileUncollapsed is the core of getAndCacheTile (and is used by it)
// without the request collapsing. Use getAndCacheTile instead of this method.
func (tch *tileCachingHandler) getAndCacheTileUncollapsed(ctx context.Context, tile tile) (*entries, tileSource, error) {
	beginS3Get := time.Now()
	contents, err := tch.getFromS3(ctx, tile)
	tch.backendLatencyMetric.WithLabelValues("s3_get").Observe(time.Since(beginS3Get).Seconds())

	if err == nil {
		return contents, sourceS3, nil
	}

	if !errors.Is(err, noSuchKey{}) {
		tch.requestsMetric.WithLabelValues("error", "s3_get").Inc()
		return nil, sourceS3, fmt.Errorf("error reading tile from s3: %w", err)
	}

	beginCTLogGet := time.Now()
	contents, err = getTileFromBackend(ctx, tile)
	tch.backendLatencyMetric.WithLabelValues("ct_log_get").Observe(time.Since(beginCTLogGet).Seconds())

	if err != nil {
		var statusCodeErr statusCodeError
		// Requests for tiles past the end of the log will get a 400 from CTFE, so report those
		// separately.
		if errors.As(err, &statusCodeErr) && statusCodeErr.statusCode == http.StatusBadRequest {
			tch.requestsMetric.WithLabelValues("bad_request", "ct_log_get").Inc()
		} else {
			tch.requestsMetric.WithLabelValues("error", "ct_log_get").Inc()
		}
		return nil, sourceCTLog, fmt.Errorf("error reading tile from backend: %w", err)
	}

	// If we got a partial tile, assume we are at the end of the log and the last
	// tile isn't filled up yet. In that case, don't write to S3, but still return
	// results to the user.
	if tch.isPartialTile(contents) {
		tch.partialTiles.Inc()
		return contents, sourceCTLog, nil
	}

	beginS3Put := time.Now()
	err = tch.writeToS3(ctx, tile, contents)
	tch.backendLatencyMetric.WithLabelValues("s3_put").Observe(time.Since(beginS3Put).Seconds())

	if err != nil {
		tch.requestsMetric.WithLabelValues("error", "s3_put").Inc()
		return nil, sourceCTLog, fmt.Errorf("error writing tile to S3: %w", err)
	}

	return contents, sourceCTLog, nil
}

// isPartialTile returns true if there are fewer items in the tile than were
// requested by the tileCachingHandler.
func (tch *tileCachingHandler) isPartialTile(contents *entries) bool {
	return len(contents.Entries) < tch.tileSize
}

// singleflightDo is a wrapper around singleflight.Group.Do that, instead of
// returning an interface{}, returns the exact type of the first return type of
// the function fn. (singleflight was built before generics)
func singleflightDo[V any](group *singleflight.Group, key string, fn func() (V, error)) (V, error, bool) {
	out, err, shared := group.Do(key, func() (interface{}, error) {
		return fn()
	})
	return out.(V), err, shared
}

// passthroughHandler is an HTTP handler that passes through GET requests to the CT log.
type passthroughHandler struct {
	logURL string
}

func (p passthroughHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintln(w, "only GET is supported")
		return
	}
	url := fmt.Sprintf("%s%s", p.logURL, r.URL.Path)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "creating request: %s\n", err)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "fetching %s: %s\n", url, err)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("copying response body to client: %s\n", err)
	}
}

func main() {
	logURL := flag.String("log-url", "", "CT log URL. e.g. https://oak.ct.letsencrypt.org/2023")
	tileSize := flag.Int("tile-size", 0, "tile size. Must match the value used by the backend")
	s3bucket := flag.String("s3-bucket", "", "s3 bucket to use for caching")
	s3prefix := flag.String("s3-prefix", "", "prefix for s3 keys. defaults to value of -backend")
	listenAddress := flag.String("listen-address", ":7962", "address to listen on")
	metricsAddress := flag.String("metrics-address", ":7963", "address to listen on for metrics")

	// fullRequestTimeout is the max allowed time the handler can read from S3 and return or read from S3, read from backend, write to S3, and return.
	fullRequestTimeout := flag.Duration("full-request-timeout", 4*time.Second, "max time to spend in the HTTP handler")

	flag.Parse()

	if *logURL == "" {
		log.Fatal("missing required flag: -log-url")
	}

	if *s3bucket == "" {
		log.Fatal("missing required flag: -s3-bucket")
	}

	if *tileSize == 0 {
		log.Fatal("missing required flag: -tile-size")
	}

	if *fullRequestTimeout == 0 {
		log.Fatal("-full-request-timeout may not have a timeout value of 0")
	}

	if *s3prefix == "" {
		*s3prefix = *logURL
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	svc := s3.NewFromConfig(cfg)

	promRegistry := newStatsRegistry(*metricsAddress)

	handler, err := newTileCachingHandler(*logURL, *tileSize, svc, *s3prefix, *s3bucket, *fullRequestTimeout, promRegistry)
	if err != nil {
		log.Fatal(err)
	}

	srv := http.Server{
		Addr:              *listenAddress,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      *fullRequestTimeout + 1*time.Second, // must be a bit larger than the max time spent in the HTTP handler
		IdleTimeout:       5 * time.Minute,
		ReadHeaderTimeout: 2 * time.Second,
		Handler:           handler,
	}

	log.Fatal(srv.ListenAndServe())
}

func newStatsRegistry(listenAddress string) prometheus.Registerer {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(
		collectors.ProcessCollectorOpts{}))

	server := http.Server{
		Addr:              listenAddress,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       5 * time.Minute,
		ReadHeaderTimeout: 2 * time.Second,
		Handler:           promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Printf("unable to start metrics server on %s: %s\n", listenAddress, err)
			os.Exit(1)
		}
	}()
	return registry
}
