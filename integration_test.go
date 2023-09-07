package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

const containerName string = "ctile_integration_test_minio"
const testLogSaysPastTheEnd string = "oh no! we fell off the end of the log!"

func startContainer(t *testing.T) {
	_, err := exec.Command("podman", "run", "--rm", "--detach", "-p", "19085:9000", "--name", containerName, "quay.io/minio/minio", "server", "/data").Output()
	if err != nil {
		t.Fatalf("minio failed to come up: %v", err)
	}
	for i := 0; i < 1000; i++ {
		_, err := net.Dial("tcp", "localhost:19085")
		if errors.Is(err, syscall.ECONNREFUSED) {
			t.Log("sleeping 10ms waiting for minio to come up")
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("failed to connect to minio: %v", err)
		}
		t.Log("minio is up")
		return
	}
	t.Fatalf("failed to connect to minio: %v", err)
}

// cleanupContainer stops a running named container and removes its assigned
// name. This is helpful in the event that a container wasn't properly killed
// during a previous test run or if manual testing was being performed and not
// cleaned up.
func cleanupContainer() {
	// Unconditionally stop the container.
	_, _ = exec.Command("podman", "stop", containerName).Output()

	// Unconditionally remove the container name if the operator did manual
	// container testing, but didn't clean up the name.
	_, _ = exec.Command("podman", "rm", containerName).Output()
}

func TestIntegration(t *testing.T) {
	cleanupContainer() // Clean up old containers and names just in case.
	startContainer(t)
	defer cleanupContainer()

	// A test CT server that responds to get-entries requests with appropriately JSON-formatted
	// data, where base64-decoding the LeafInput and ExtraData fields yields a binary encoding
	// of the position of the given element.
	//
	// This acts like a CT log with a max_getentries limit of 3 and 10 elements in total.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startInt, _ := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
		endInt, _ := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
		var entries entries

		// Behave as if the CT server has a max_get_entries limit of 3.
		// The +1 and -1 are because CT uses closed intervals.
		if endInt-startInt+1 > 3 {
			endInt = startInt + 3 - 1
		}

		// Behave as if the CT server has a total of 10 entries
		if startInt > 10 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(testLogSaysPastTheEnd))
			return
		}

		if endInt > 10 {
			endInt = 10
		}

		for i := startInt; i <= endInt; i++ {
			leafInput := make([]byte, 8)
			binary.PutVarint(leafInput, i)
			extraData := make([]byte, 8)
			binary.PutVarint(extraData, i)
			entries.Entries = append(entries.Entries, entry{
				LeafInput: leafInput,
				ExtraData: extraData,
			})
		}

		encoder := json.NewEncoder(w)
		encoder.Encode(entries)
	}))
	defer server.Close()

	const defaultRegion = "fakeRegion"
	hostAddress := "http://localhost:19085"

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               hostAddress,
			SigningRegion:     defaultRegion,
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(defaultRegion),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	s3Service := s3.NewFromConfig(cfg)

	_, err = s3Service.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("bucket"),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctile := tileCachingHandler{
		logURL:   server.URL,
		tileSize: 3,

		s3Service: s3Service,
		s3Prefix:  "test",
		s3Bucket:  "bucket",

		cacheGroup: &singleflight.Group{},

		requestsMetric:     prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"result", "source"}),
		partialTiles:       prometheus.NewCounter(prometheus.CounterOpts{}),
		singleFlightShared: prometheus.NewCounter(prometheus.CounterOpts{}),
	}

	// Invalid URL; should 404
	resp := getResp(ctile, "/foo")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404 got %d", resp.StatusCode)
	}

	// Malformed queries; should 400
	malformed := []string{
		"/ct/v1/get-entries?start=a&end=b",
		"/ct/v1/get-entries?start=1&end=b",
		"/ct/v1/get-entries?start=a&end=1",
		"/ct/v1/get-entries?start=1&end=0",
		"/ct/v1/get-entries?start=-1&end=1",
		"/ct/v1/get-entries?start=1&end=-1",
		"/ct/v1/get-entries?start=1",
		"/ct/v1/get-entries?end=1",
	}
	for _, m := range malformed {
		resp := getResp(ctile, m)
		if resp.StatusCode != 400 {
			t.Errorf("%q: expected 400 got %d", m, resp.StatusCode)
		}
	}

	// Valid query; should 200
	twoEntriesA, headers, err := getAndParseResp(t, ctile, "/ct/v1/get-entries?start=3&end=4")
	if err != nil {
		t.Error(err)
	}

	expectHeader(t, headers, "X-Source", "CT log")

	if len(twoEntriesA.Entries) != 2 {
		t.Errorf("expected 2 entries got %d", len(twoEntriesA.Entries))
	}

	// Same query again; should come from S3 this time.
	twoEntriesB, headers, err := getAndParseResp(t, ctile, "/ct/v1/get-entries?start=3&end=4")
	if err != nil {
		t.Error(err)
	}

	expectHeader(t, headers, "X-Source", "S3")

	if len(twoEntriesB.Entries) != 2 {
		t.Errorf("expected 2 entries got %d", len(twoEntriesB.Entries))
	}

	// The results from the first and second queries should be the same
	if !reflect.DeepEqual(twoEntriesA, twoEntriesB) {
		t.Errorf("expected equal responses got %#v != %#v", twoEntriesA, twoEntriesB)
	}

	// The third entry in this first tile should also be served from S3 now, because it
	// was pulled into cache by the previous requests.
	oneEntry, headers, err := getAndParseResp(t, ctile, "/ct/v1/get-entries?start=5&end=5")
	if err != nil {
		t.Error(err)
	}

	expectHeader(t, headers, "X-Source", "S3")

	if len(oneEntry.Entries) != 1 {
		t.Errorf("expected 1 entry got %d", len(oneEntry.Entries))
	}

	// Tiles fetched from the end of the log will be partial. CTile should not cache.
	_, headers, err = getAndParseResp(t, ctile, "/ct/v1/get-entries?start=9&end=11")
	if err != nil {
		t.Error(err)
	}

	expectHeader(t, headers, "X-Source", "CT log")

	_, headers, err = getAndParseResp(t, ctile, "/ct/v1/get-entries?start=9&end=11")
	if err != nil {
		t.Error(err)
	}

	// This should still come from the CT log rather than from S3, even though it was
	// requested twice in a row.
	expectHeader(t, headers, "X-Source", "CT log")

	// Tiles fetched past the end of the log will get a 400 from our test CT log; ctile
	// should pass that through, along with the body.
	resp = getResp(ctile, "/ct/v1/get-entries?start=99&end=100")
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), testLogSaysPastTheEnd) {
		t.Errorf("expected response to contain %q got %q", testLogSaysPastTheEnd, body)
	}

	// A request where the _tile_ starts inside the log but the requested `start` value is
	// outside the log. In this case ctile synthesizes a 400.
	resp = getResp(ctile, "/ct/v1/get-entries?start=11&end=12")
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 got %d", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	pastTheEnd := "requested range is past the end of the log"
	if !strings.Contains(string(body), pastTheEnd) {
		t.Errorf("expected response to contain %q got %q", pastTheEnd, body)
	}
}

func getResp(ctile tileCachingHandler, url string) *http.Response {
	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()

	ctile.ServeHTTP(w, req)

	return w.Result()
}

func getAndParseResp(t *testing.T, ctile tileCachingHandler, url string) (entries, http.Header, error) {
	t.Helper()
	resp := getResp(ctile, url)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%q: expected status code 200 got %d with body: %q", url, resp.StatusCode, body)
	}
	var entries entries
	err := json.Unmarshal(body, &entries)
	return entries, resp.Header, err
}

func expectHeader(t *testing.T, headers http.Header, key, expected string) {
	t.Helper()
	if headers.Get(key) != expected {
		t.Errorf("header %q: expected %q got %q", key, expected, headers.Get(key))
	}
}
