// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// The API refuses a name that is already in use -- returnCode 1103047 for
// a volume, 1100015 for a snapshot, both HTTP 400 (measured on the live
// gateway, 2026-10-06). That is the one rejection a create must not read
// as "not created": the resource exists, made by a request this one did
// not see, and the driver returns it instead of failing.
func TestCreateClassifiesANameInUse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		code, msg := "1103047", "The storage instance name is already in use. Please use a different name."
		if strings.HasSuffix(r.URL.Path, "/createBlockStorageSnapshotInstance") {
			code, msg = "1100015", "The instance name is already in use. Please use a different name."
		}
		_, _ = io.WriteString(w, `{"responseError":{"returnCode":"`+code+`","returnMessage":"`+msg+`"}}`)
	}))
	defer srv.Close()

	c, err := NewNcloudCloud("k", "s", "KR", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = c.CreateVolume(ctx, &VolumeOptions{Name: "pvc-a", SizeGiB: 10, VolumeType: "CB1", ZoneCode: "KR-1"})
	if !errors.Is(err, ErrNameInUse) {
		t.Errorf("CreateVolume on returnCode 1103047: want ErrNameInUse, got %v", err)
	}
	// Still an API rejection by shape; callers check ErrNameInUse first.
	if !IsRejectedByAPI(err) {
		t.Errorf("a 400 answer should read as rejected by the API: %v", err)
	}

	_, err = c.CreateSnapshot(ctx, &SnapshotOptions{Name: "snap-a", SourceVolumeID: "9000001"})
	if !errors.Is(err, ErrNameInUse) {
		t.Errorf("CreateSnapshot on returnCode 1100015: want ErrNameInUse, got %v", err)
	}
}

// Any other 4xx is a plain rejection: nothing was created, and the error
// must not be mistaken for a name collision.
func TestCreateKeepsOtherRejectionsApart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"responseError":{"returnCode":"1000020","returnMessage":"Invalid parameter"}}`)
	}))
	defer srv.Close()

	c, err := NewNcloudCloud("k", "s", "KR", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateVolume(context.Background(), &VolumeOptions{Name: "pvc-a", SizeGiB: 10, VolumeType: "CB1", ZoneCode: "KR-1"})
	if err == nil || errors.Is(err, ErrNameInUse) {
		t.Errorf("want a plain error for returnCode 1000020, got %v", err)
	}
	if !IsRejectedByAPI(err) {
		t.Errorf("a 400 answer should read as rejected by the API: %v", err)
	}
	if got := apiReturnCode(err); got != "1000020" {
		t.Errorf("apiReturnCode = %q, want 1000020", got)
	}
}

// IsRejectedByAPI separates "the API said no" from "the outcome is
// unknown". Only the former may become a final error for a create: a
// transport failure, a timeout, a 5xx or an unparsable answer may all
// hide a resource the backend did create.
func TestIsRejectedByAPI(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{"HTTP 400 body", errors.New(`Status: 400 Bad Request, Body: {"responseError": {"returnCode": "1000020"}}`), true},
		{"wrapped HTTP 400", fmt.Errorf("failed to create block storage: %w", errors.New(`Status: 400 Bad Request, Body: {}`)), true},
		{"HTTP 403", errors.New(`Status: 403 Forbidden, Body: {"error":{"errorCode":"200","message":"Authentication Failed"}}`), true},
		{"HTTP 503", errors.New(`Status: 503 Service Unavailable, Body: upstream connect error`), false},
		{"client timeout", &url.Error{Op: "Post", URL: "https://api.invalid/", Err: context.DeadlineExceeded}, false},
		{"connection refused", &url.Error{Op: "Post", URL: "https://api.invalid/", Err: errors.New("dial tcp: connection refused")}, false},
		{"unparsed 2xx", fmt.Errorf("createBlockStorageInstance: %w", errUnparsedAPIResponse), false},
		{"2xx error envelope", &apiError{op: "createBlockStorageInstance", code: "1101015"}, false},
		{"nil", nil, false},
	} {
		if got := IsRejectedByAPI(tt.err); got != tt.want {
			t.Errorf("%s: IsRejectedByAPI = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// apiReturnCode reads the code out of both shapes an API error takes.
func TestAPIReturnCodeReadsBothShapes(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{
		{"4xx body text", errors.New(`Status: 400 Bad Request, Body: {"responseError": {"returnCode": "1101015", "returnMessage": "x"}}`), "1101015"},
		{"4xx body text, gateway spacing", errors.New("Status: 400 Bad Request, Body: {\n  \"responseError\" : {\n    \"returnCode\" : \"1103047\"\n  }\n}"), "1103047"},
		{"2xx envelope", &apiError{op: "x", code: "1100015"}, "1100015"},
		{"wrapped 2xx envelope", fmt.Errorf("create snapshot: %w", &apiError{op: "x", code: "1100015"}), "1100015"},
		{"transport", &url.Error{Op: "Post", URL: "https://api.invalid/", Err: context.DeadlineExceeded}, ""},
		{"nil", nil, ""},
	} {
		if got := apiReturnCode(tt.err); got != tt.want {
			t.Errorf("%s: apiReturnCode = %q, want %q", tt.name, got, tt.want)
		}
	}
}

// apiUrl is the gateway origin; the service path is appended by the client.
// A trailing slash on the origin must not produce "//vserver/v2".
func TestAPIURLTrailingSlashIsTolerated(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, wrapAPIResponse("getBlockStorageInstanceDetailResponse",
			`{"returnCode":"0","returnMessage":"success","totalRows":0,"blockStorageInstanceList":[]}`))
	}))
	defer srv.Close()

	c, err := NewNcloudCloud("k", "s", "KR", srv.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetVolumeByID(context.Background(), "9000001"); !errors.Is(err, ErrVolumeNotFound) {
		t.Fatalf("want ErrVolumeNotFound from an empty list, got %v", err)
	}
	if !strings.HasPrefix(path, "/vserver/v2/") || strings.Contains(path, "//") {
		t.Errorf("request path = %q, want /vserver/v2/<action>", path)
	}
}
