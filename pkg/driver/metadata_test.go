// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// The real IMDS terminates every value with a newline. The client must hand
// back clean values: zoneCode ends up in topology.kubernetes.io/zone, which
// rejects whitespace (kubelet: "detected topology value collision").
func TestMetadataClientTrimsValues(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/latest/api/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.Header.Get("X-NCP-METADATA-TOKEN-TTL-SECONDS") == "" {
			http.Error(w, "bad token request", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "test-token")
	})
	mux.HandleFunc("/latest/meta-data/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-NCP-METADATA-TOKEN") != "test-token" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/latest/meta-data/serverInstanceNo":
			fmt.Fprint(w, "200000001\n")
		case "/latest/meta-data/zoneCode":
			fmt.Fprint(w, "KR-2\n")
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Setenv("NCLOUD_METADATA_API_ENDPOINT", srv.URL)

	serverInstanceNo, zoneCode, err := getNodeInfoFromMetadata()
	if err != nil {
		t.Fatalf("getNodeInfoFromMetadata: %v", err)
	}
	if serverInstanceNo != "200000001" || zoneCode != "KR-2" {
		t.Fatalf("got %q/%q, want trimmed 200000001/KR-2", serverInstanceNo, zoneCode)
	}
}

// Startup reads two values. The token is acquired once and sent with both
// requests; the previous client paid a token PUT per value.
func TestMetadataClientReusesToken(t *testing.T) {
	const token = "test-token"
	var tokenRequests, tokenlessGets int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			atomic.AddInt32(&tokenRequests, 1)
			_, _ = w.Write([]byte(token))
		case r.Header.Get("X-NCP-METADATA-TOKEN") != token:
			atomic.AddInt32(&tokenlessGets, 1)
			w.WriteHeader(http.StatusUnauthorized)
		case strings.HasSuffix(r.URL.Path, "/serverInstanceNo"):
			_, _ = w.Write([]byte("200000001\n"))
		case strings.HasSuffix(r.URL.Path, "/zoneCode"):
			_, _ = w.Write([]byte("KR-2\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("NCLOUD_METADATA_API_ENDPOINT", srv.URL)

	serverInstanceNo, zoneCode, err := getNodeInfoFromMetadata()
	if err != nil {
		t.Fatalf("getNodeInfoFromMetadata: %v", err)
	}
	if serverInstanceNo != "200000001" || zoneCode != "KR-2" {
		t.Fatalf("metadata = %q/%q", serverInstanceNo, zoneCode)
	}
	if got := atomic.LoadInt32(&tokenRequests); got != 1 {
		t.Errorf("got %d token requests, want 1", got)
	}
	if got := atomic.LoadInt32(&tokenlessGets); got != 0 {
		t.Errorf("got %d requests without the token, want 0", got)
	}
}

// A node whose token endpoint is missing or broken still serves the values
// on a plain GET (checked on a live node), so the driver must start there
// instead of failing on the token.
func TestMetadataClientFallsBackWithoutToken(t *testing.T) {
	var tokenRequests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			atomic.AddInt32(&tokenRequests, 1)
			w.WriteHeader(http.StatusInternalServerError)
		case r.Header.Get("X-NCP-METADATA-TOKEN") != "":
			w.WriteHeader(http.StatusUnauthorized)
		case strings.HasSuffix(r.URL.Path, "/serverInstanceNo"):
			_, _ = w.Write([]byte("200000001\n"))
		case strings.HasSuffix(r.URL.Path, "/zoneCode"):
			_, _ = w.Write([]byte("KR-2\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("NCLOUD_METADATA_API_ENDPOINT", srv.URL)

	serverInstanceNo, zoneCode, err := getNodeInfoFromMetadata()
	if err != nil {
		t.Fatalf("startup should fall back to token-less requests: %v", err)
	}
	if serverInstanceNo != "200000001" || zoneCode != "KR-2" {
		t.Fatalf("metadata = %q/%q", serverInstanceNo, zoneCode)
	}
	if atomic.LoadInt32(&tokenRequests) == 0 {
		t.Error("the token must still be requested before falling back")
	}
}

// The cached token stops being accepted (expired or revoked): the client
// renews it once and retries instead of failing the read.
func TestMetadataClientRenewsRejectedToken(t *testing.T) {
	var tokenRequests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			n := atomic.AddInt32(&tokenRequests, 1)
			_, _ = w.Write([]byte(fmt.Sprintf("token-%d", n)))
		case r.Header.Get("X-NCP-METADATA-TOKEN") != "token-2":
			// Only the second token is accepted, so the first read gets 401
			// with token-1 and must succeed after renewing.
			w.WriteHeader(http.StatusUnauthorized)
		case strings.HasSuffix(r.URL.Path, "/serverInstanceNo"):
			_, _ = w.Write([]byte("200000001\n"))
		case strings.HasSuffix(r.URL.Path, "/zoneCode"):
			_, _ = w.Write([]byte("KR-2\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("NCLOUD_METADATA_API_ENDPOINT", srv.URL)

	serverInstanceNo, zoneCode, err := getNodeInfoFromMetadata()
	if err != nil {
		t.Fatalf("startup should renew a rejected token: %v", err)
	}
	if serverInstanceNo != "200000001" || zoneCode != "KR-2" {
		t.Fatalf("metadata = %q/%q", serverInstanceNo, zoneCode)
	}
	if got := atomic.LoadInt32(&tokenRequests); got != 2 {
		t.Errorf("got %d token requests, want 2 (initial + one renewal)", got)
	}
}
