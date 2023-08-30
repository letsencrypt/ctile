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
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/fxamacker/cbor/v2"
)

// parseQueryParams returns the start and end values, or an error.
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
		return 0, 0, errors.New("invalid start parameter")
	}
	endInt, err := strconv.ParseInt(end, 10, 64)
	if err != nil || endInt < 0 {
		return 0, 0, errors.New("invalid end parameter")
	}
	if endInt <= startInt {
		return 0, 0, errors.New("end must be greater than start")
	}
	return startInt, endInt, nil
}

// tile represents important numbers about a tile: where it starts, where it ends, its size,
// and what CT backend URL it exists on (or is anticipated to exist on), and what s3 prefix
// it should be stored/retrieved under.
type tile struct {
	start    int64
	end      int64
	size     int64
	logURL   string
	s3prefix string
}

// makeTile returns a tile of size `size` that contains the given `start` position.
// The resulting tile's `start` will be equal to or less than the requested `start`.
func makeTile(start, size int64, backend string, s3prefix string) tile {
	tileOffset := start % size
	tileStart := start - tileOffset
	return tile{
		start:    tileStart,
		end:      tileStart + size,
		size:     size,
		logURL:   backend,
		s3prefix: s3prefix,
	}
}

// key returns the S3 key for the tile.
func (t tile) key() string {
	return fmt.Sprintf("%s/tile_size=%d/%d.cbor.gz", t.s3prefix, t.size, t.start)
}

// entries corresponds to the JSON response to the CT get-entries endpoint.
// https://datatracker.ietf.org/doc/html/rfc6962#section-4.6
//
// It is marshaled and unmarshaled to/from JSON and CBOR.
type entries struct {
	Entries []entry `json:"entries"`
}

// entry corresponds to a single entry in the CT get-entries endpoint.
//
// Note: the JSON fields are base64. For fields of type `[]byte`, Go's encoding/json
// automagically decodes base64.
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
func getTileFromBackend(ctx context.Context, path string, t tile) (*entries, error) {
	url := fmt.Sprintf("%s/%s?start=%d&end=%d", t.logURL, path, t.start, t.end)
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create backend Request object: %s", err)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %s", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading body from %s: %s", url, err)
		}
		return nil, statusCodeError{resp.StatusCode, body}
	}

	var entries entries
	err = json.NewDecoder(resp.Body).Decode(&entries)
	if err != nil {
		return nil, fmt.Errorf("reading body from %s: %s", url, err)
	}

	if len(entries.Entries) > int(t.size) || len(entries.Entries) == 0 {
		return nil, fmt.Errorf("expected %d entries, got %d", t.size, len(entries.Entries))
	}

	return &entries, nil
}

// writeToS3 stores the entries corresponding to the given tile in s3.
func writeToS3(ctx context.Context, svc *s3.S3, bucket string, t tile, e *entries) error {
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
		return fmt.Errorf("closing gzip writer: %s", err)
	}

	key := t.key()
	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("putting in bucket %q with key %q: %s", bucket, key, err)
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
func getFromS3(ctx context.Context, svc *s3.S3, bucket string, t tile) (*entries, error) {
	key := t.key()
	resp, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return nil, noSuchKey{}
		}
		return nil, fmt.Errorf("getting from bucket %q with key %q: %s", bucket, key, err)
	}

	var entries entries
	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("making gzipReader: %s", err)
	}
	err = cbor.NewDecoder(gzipReader).Decode(&entries)
	if err != nil {
		return nil, fmt.Errorf("reading body from bucket %q with key %q: %s", bucket, key, err)
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
	tileSize int    // The CT tile size used here and in the given backend. Must not be zero.

	s3Service *s3.S3 // The S3 service to use for caching tiles. Must not be nil.
	s3Prefix  string // The prefix to add to the path when caching tiles in S3. Must not be empty.
	s3Bucket  string // The S3 bucket to use for caching tiles. Must not be empty.
}

func (tch *tileCachingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/ct/v1/get-entries") {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "invalid path %q\n", r.URL.Path)
		return
	}

	start, end, err := parseQueryParams(r.URL.Query())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, err)
		return
	}

	tile := makeTile(start, int64(tch.tileSize), tch.logURL, tch.s3Prefix)

	contents, err := getFromS3(r.Context(), tch.s3Service, tch.s3Bucket, tile)
	if err != nil && errors.Is(err, noSuchKey{}) {
		contents, err = getTileFromBackend(r.Context(), r.URL.Path, tile)
		if err != nil {
			status := http.StatusInternalServerError
			var statusCodeErr statusCodeError
			if errors.As(err, &statusCodeErr) {
				status = statusCodeErr.statusCode
			}
			w.WriteHeader(status)
			fmt.Fprintln(w, err)
			return
		}

		// If we go a partial tile, assume we are at the end of the log and the last
		// tile isn't filled up yet. In that case, don't write to S3, but still return
		// results to the user.
		if len(contents.Entries) == tch.tileSize {
			err := writeToS3(r.Context(), tch.s3Service, tch.s3Bucket, tile, contents)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "writing to s3: %s\n", err)
				return
			}
		} else {
			w.Header().Set("X-Partial-Tile", "true")
		}

		w.Header().Set("X-Source", "CT log")
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "reading from s3: %s\n", err)
		return
	} else {
		w.Header().Set("X-Source", "S3")
	}

	// Truncate to match the request
	prefixToRemove := start - tile.start
	contents.Entries = contents.Entries[prefixToRemove:]

	requestedLen := end - start
	if len(contents.Entries) > int(requestedLen) {
		contents.Entries = contents.Entries[:requestedLen]
	}

	w.Header().Set("X-Response-Len", fmt.Sprintf("%d", len(contents.Entries)))
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.Encode(contents)
}

func main() {
	logURL := flag.String("log-url", "", "CT log URL. e.g. https://oak.ct.letsencrypt.org/2023")
	tileSize := flag.Int("tile-size", 0, "tile size. Must match the value used by the backend")
	s3bucket := flag.String("s3-bucket", "", "s3 bucket to use for caching")
	s3prefix := flag.String("s3-prefix", "", "prefix for s3 keys. defaults to value of -backend")
	listenAddress := flag.String("listen-address", ":8080", "address to listen on")

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

	sess := session.Must(session.NewSession())
	svc := s3.New(sess)

	handler := &tileCachingHandler{
		logURL:    *logURL,
		tileSize:  *tileSize,
		s3Service: svc,
		s3Prefix:  *s3prefix,
		s3Bucket:  *s3bucket,
	}

	srv := http.Server{
		Addr:              *listenAddress,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      *fullRequestTimeout + 1*time.Second, // must be a bit larger than than than the max time spent in the HTTP handler
		IdleTimeout:       5 * time.Minute,
		ReadHeaderTimeout: 2 * time.Second,
		Handler:           http.TimeoutHandler(handler, *fullRequestTimeout, "full request timeout"),
	}

	log.Fatal(srv.ListenAndServe())
}
