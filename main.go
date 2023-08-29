package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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

type tile struct {
	start int64
	end   int64
	size  int64
}

func (t tile) key() string {
	return fmt.Sprintf("%d.cbor.gz", t.start)
}

type entry struct {
	LeafInput []byte `json:"leaf_input"`
	ExtraData []byte `json:"extra_data"`
}

type entries struct {
	Entries []entry `json:"entries"`
}

func makeTile(start, end, size int64) tile {
	tileOffset := start % size
	tileStart := start - tileOffset
	return tile{
		start: tileStart,
		end:   tileStart + size,
		size:  size,
	}
}

// getTileFromBackend fetches a tile of entries from the backend, of size tileSize.
//
// The returned start value represents the start of the tile, and is guaranteed to
// be equal or less than the requested start. The returned end value represents the
// end of the tile, and is guaranteed to be `start + tileSize`.
func getTileFromBackend(base, path string, t tile) (*entries, error) {
	url := fmt.Sprintf("%s/%s?start=%d&end=%d", base, path, t.start, t.end)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %s", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching %s: status code %d", url, resp.StatusCode)
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
func writeToS3(svc *s3.S3, bucket string, t tile, e *entries) error {
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
	ctx := context.TODO()
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
func getFromS3(svc *s3.S3, bucket string, t tile) (*entries, error) {
	key := t.key()
	resp, err := svc.GetObject(&s3.GetObjectInput{
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

func main() {
	backend := flag.String("backend", "https://oak.ct.letsencrypt.org/2023", "backend URL")
	tileSize := flag.Int("tile-size", 256, "tile size. Must match the value used by the backend.")
	s3bucket := flag.String("s3-bucket", "", "s3 bucket to use for caching")
	listenAddress := flag.String("listen-address", ":8080", "address to listen on")

	flag.Parse()

	if *s3bucket == "" {
		log.Fatal("missing required flag: -s3-bucket")
	}

	sess := session.Must(session.NewSession())
	svc := s3.New(sess)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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

		tile := makeTile(start, end, int64(*tileSize))

		contents, err := getFromS3(svc, *s3bucket, tile)
		if err != nil && errors.Is(err, noSuchKey{}) {
			contents, err = getTileFromBackend(*backend, r.URL.Path, tile)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintln(w, err)
				return
			}

			if len(contents.Entries) == *tileSize {
				err := writeToS3(svc, *s3bucket, tile, contents)
				if err != nil {
					// TODO: This should log the error but not return it to the user.
					// In particular, errors due to the contents being less than a full
					// tile in size should be ignored, since that will commonly happen when
					// requesting ranges near the current end of the log.
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintf(w, "writing to s3: %s\n", err)
					return
				}
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

		if r.URL.Query().Get("format") == "cbor" {
			encoder := cbor.NewEncoder(w)
			encoder.Encode(contents)
			return
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(contents)
	})

	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
