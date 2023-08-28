package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const backend = "https://oak.ct.letsencrypt.org"
const tileSize = 256
const s3bucket = "oak-crud-net"

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
	return startInt, endInt, nil
}

type tile struct {
	start int64
	end   int64
	size  int64
}

type entry struct {
	LeafInput string `json:"leaf_input"`
	ExtraData string `json:"extra_data"`
}

type entries struct {
	Entries []entry `json:"entries"`
}

func makeTile(start, end, size int64) tile {
	tileOffset := start % tileSize
	tileStart := start - tileOffset
	return tile{
		start: tileStart,
		end:   tileStart + tileSize,
		size:  tileSize,
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

	if len(entries.Entries) > int(tileSize) || len(entries.Entries) == 0 {
		return nil, fmt.Errorf("expected %d entries, got %d", tileSize, len(entries.Entries))
	}

	return &entries, nil
}

// writeToS3 stores the entries corresponding to the given tile in s3.
func writeToS3(svc *s3.S3, t tile, e *entries) error {
	if len(e.Entries) != int(t.size) || t.end != t.start+t.size {
		return fmt.Errorf("internal inconsistency: len(entries) == %d; tile = %v", len(e.Entries), t)
	}

	body, err := json.Marshal(e)
	if err != nil {
		return nil
	}

	key := fmt.Sprintf("%d", t.start)
	ctx := context.TODO()
	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s3bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("putting in bucket %q with key %q: %s", s3bucket, key, err)
	}
	return nil
}

// getFromS3 retrieves the entries corresponding to the given tile from s3.
func getFromS3(svc *s3.S3, t tile) (*entries, error) {
	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3bucket),
		Key:    aws.String(fmt.Sprintf("%d", t.start)),
	})
	if err != nil {
		return nil, fmt.Errorf("getting from bucket %q with key %q: %s", s3bucket, t.start, err)
	}

	var entries entries
	err = json.NewDecoder(resp.Body).Decode(&entries)
	if err != nil {
		return nil, fmt.Errorf("reading body from bucket %q with key %q: %s", s3bucket, t.start, err)
	}

	if len(entries.Entries) != int(t.size) || t.end != t.start+t.size {
		return nil, fmt.Errorf("internal inconsistency: len(entries) == %d; tile = %v", len(entries.Entries), t)
	}

	return &entries, nil
}

func main() {
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

		tile := makeTile(start, end, tileSize)

		contents, err := getFromS3(svc, tile)
		if err != nil {
			contents, err = getTileFromBackend(backend, r.URL.Path, tile)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintln(w, err)
				return
			}

			if len(contents.Entries) == tileSize {
				err := writeToS3(svc, tile, contents)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintf(w, "writing to s3: %s\n", err)
					return
				}
			}

			w.Header().Set("X-Source", "CT log")
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

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(contents)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
