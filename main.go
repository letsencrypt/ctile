package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

const backend = "https://oak.ct.letsencrypt.org/2023"
const tileSize = 256

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/ct/v1/get-entries") {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "invalid path %q", r.URL.Path)
			return
		}
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")
		if start == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing start parameter"))
			return
		}
		if end == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing end parameter"))
			return
		}
		startInt, err := strconv.ParseInt(start, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid start parameter"))
			return
		}
		endInt, err := strconv.ParseInt(end, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid start parameter"))
			return
		}
		fmt.Printf("%d\n", endInt) // XXX

		tileStart := startInt / tileSize
		tileEnd := tileStart + tileSize
		tileOffset := startInt % tileSize

		url := fmt.Sprintf("%s/%s?start=%d&end=%d", backend, r.URL.Path, tileStart, tileEnd)
		resp, err := http.Get(url)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprintf(w, "fetching %s: %s", url, err)
		}

		if resp.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "fetching %s: status code %d", url, resp.StatusCode)
		}

		type entry struct {
			LeafInput string `json:"leaf_input"`
			ExtraData string `json:"extra_data"`
		}

		var entries struct {
			Entries []entry `json:"entries"`
		}

		err = json.NewDecoder(resp.Body).Decode(&entries)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "reading body from %s: %s", url, err)
		}

		if len(entries.Entries) != tileSize {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "expected %d entries, got %d", tileSize, len(entries.Entries))
		}

		// Truncate to match the request
		entries.Entries = entries.Entries[tileOffset:]
		if len(entries.Entries) > int(endInt)-int(startInt) {
			entries.Entries = entries.Entries[:endInt-startInt]
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(entries)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))

}
