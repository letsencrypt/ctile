package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const backend = "https://oak.ct.letsencrypt.org"
const tileSize = 256
const s3bucket = "ctile"

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
	start    int64
	end      int64
	contents entries
}

type entry struct {
	LeafInput string `json:"leaf_input"`
	ExtraData string `json:"extra_data"`
}

type entries struct {
	Entries []entry `json:"entries"`
}

// getTile fetches a tile of entries from the backend, of size tileSize.
//
// The returned start value represents the start of the tile, and is guaranteed to
// be equal or less than the requested start. The returned end value represents the
// end of the tile, and is guaranteed to be `start + tileSize`.
func getTile(base, path string, start, end, tileSize int64) (*tile, error) {
	tileOffset := start % tileSize
	tileStart := start - tileOffset
	tileEnd := tileStart + tileSize

	url := fmt.Sprintf("%s/%s?start=%d&end=%d", base, path, tileStart, tileEnd)
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

	return &tile{
		start:    tileStart,
		end:      tileEnd,
		contents: entries,
	}, nil
}

func main() {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/ct/v1/get-entries") {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "invalid path %q", r.URL.Path)
			return
		}

		start, end, err := parseQueryParams(r.URL.Query())
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintln(w, err)
			return
		}

		tile, err := getTile(backend, r.URL.Path, start, end, tileSize) //XXX
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, err)
			return
		}

		if len(tile.contents.Entries) == tileSize {
			key := fmt.Sprintf("%d", tile.start)
			ctx := context.TODO()
			_, err := svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: aws.String(s3bucket),
				Key:    aws.String(key),
				Body:   os.Stdin,
			})
			if err != nil {
				log.Printf("putting in s3 bucket %q with key %q: %s", s3bucket, key, err)
			}
		}

		// Truncate to match the request
		prefixToRemove := start - tile.start
		tile.contents.Entries = tile.contents.Entries[prefixToRemove:]

		requestedLen := end - start
		if len(tile.contents.Entries) > int(requestedLen) {
			tile.contents.Entries = tile.contents.Entries[:requestedLen]
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(tile.contents)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
