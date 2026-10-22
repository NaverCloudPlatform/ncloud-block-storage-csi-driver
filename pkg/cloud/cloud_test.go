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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fullPage returns exactly listPageSize items so collectPages continues to
// the next page.
func fullPage(start int) []int {
	page := make([]int, listPageSize)
	for i := range page {
		page[i] = start + i
	}
	return page
}

func TestCollectPages(t *testing.T) {
	t.Run("drains multiple pages until totalRows", func(t *testing.T) {
		total := listPageSize + 500
		items, err := collectPages(func(pageNo int32) ([]int, int32, error) {
			switch pageNo {
			case 1:
				return fullPage(0), total, nil
			case 2:
				return fullPage(int(listPageSize))[:500], total, nil
			default:
				return nil, total, fmt.Errorf("unexpected page %d", pageNo)
			}
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if int32(len(items)) != total {
			t.Errorf("expected %d items, got %d", total, len(items))
		}
		if items[len(items)-1] != int(total)-1 {
			t.Errorf("expected last item %d, got %d", int(total)-1, items[len(items)-1])
		}
	})

	t.Run("stops when accumulated count reaches totalRows", func(t *testing.T) {
		total := 2 * listPageSize
		calls := 0
		items, err := collectPages(func(pageNo int32) ([]int, int32, error) {
			calls++
			return fullPage(int(pageNo-1) * int(listPageSize)), total, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if int32(len(items)) != total {
			t.Errorf("expected %d items, got %d", total, len(items))
		}
		if calls != 2 {
			t.Errorf("expected 2 page fetches, got %d", calls)
		}
	})

	t.Run("single short page returns immediately", func(t *testing.T) {
		items, err := collectPages(func(pageNo int32) ([]int, int32, error) {
			if pageNo != 1 {
				return nil, 1, fmt.Errorf("unexpected page %d", pageNo)
			}
			return []int{42}, 1, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(items) != 1 || items[0] != 42 {
			t.Errorf("expected [42], got %v", items)
		}
	})

	t.Run("keeps paging when the server caps the page size", func(t *testing.T) {
		// Pages come back much shorter than the requested listPageSize,
		// but totalRows reports more rows: pagination must continue.
		pages := [][]int{{0, 1}, {2, 3}, {4}}
		calls := 0
		items, err := collectPages(func(pageNo int32) ([]int, int32, error) {
			calls++
			if int(pageNo) > len(pages) {
				return nil, 5, fmt.Errorf("unexpected page %d", pageNo)
			}
			return pages[pageNo-1], 5, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(items) != 5 {
			t.Errorf("expected 5 items, got %d", len(items))
		}
		if calls != 3 {
			t.Errorf("expected 3 page fetches, got %d", calls)
		}
	})

	t.Run("empty page with more totalRows pending is an error, not a partial result", func(t *testing.T) {
		_, err := collectPages(func(pageNo int32) ([]int, int32, error) {
			if pageNo == 1 {
				return []int{1, 2}, 10, nil
			}
			return nil, 10, nil
		})
		if err == nil {
			t.Fatal("expected an error for the totalRows disagreement, got nil")
		}
	})

	t.Run("collecting more than totalRows succeeds when the list shrank mid-pagination", func(t *testing.T) {
		// Rows are deleted while paginating: the final empty page reports a
		// totalRows smaller than what was already collected. That is a
		// benign race and must not fail the listing.
		items, err := collectPages(func(pageNo int32) ([]int, int32, error) {
			switch pageNo {
			case 1:
				return fullPage(0), listPageSize + 500, nil
			case 2:
				return fullPage(int(listPageSize))[:300], listPageSize + 500, nil
			default:
				// Empty page; total shrank below the 1300 already collected.
				return nil, listPageSize + 200, nil
			}
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if int32(len(items)) != listPageSize+300 {
			t.Errorf("expected %d items, got %d", listPageSize+300, len(items))
		}
	})

	t.Run("empty first page returns empty result", func(t *testing.T) {
		items, err := collectPages(func(_ int32) ([]int, int32, error) {
			return nil, 0, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(items) != 0 {
			t.Errorf("expected no items, got %d", len(items))
		}
	})

	t.Run("propagates fetch errors", func(t *testing.T) {
		fetchErr := errors.New("api error")
		_, err := collectPages(func(pageNo int32) ([]int, int32, error) {
			if pageNo == 1 {
				return fullPage(0), 2 * listPageSize, nil
			}
			return nil, 0, fetchErr
		})
		if !errors.Is(err, fetchErr) {
			t.Errorf("expected fetch error, got %v", err)
		}
	})

	t.Run("errors out instead of looping forever", func(t *testing.T) {
		_, err := collectPages(func(_ int32) ([]int, int32, error) {
			// Full pages forever with no totalRows signal.
			return fullPage(0), 0, nil
		})
		if err == nil {
			t.Fatal("expected termination error, got nil")
		}
	})
}

// The attach-limit classification matches the live error shape: HTTP 400
// with returnCode 1101015 ("Storage allocation exceeded"), measured on the
// live API with a server at its 20-attachment limit.
func TestIsAttachmentLimitError(t *testing.T) {
	live := errors.New("Status: 400 Bad Request, Body: {\n  \"responseError\": {\n    \"returnCode\": \"1101015\",\n    \"returnMessage\": \"Storage allocation exceeded.\"\n  }\n}")
	if !isAttachmentLimitError(live) {
		t.Error("expected the measured live error to classify as attachment limit")
	}
	for _, err := range []error{
		nil,
		errors.New("Status: 400 Bad Request, Body: {\"responseError\": {\"returnCode\": \"1153003\", \"returnMessage\": \"other\"}}"),
		errors.New("plain network error: 1101015 volumes"),
	} {
		if isAttachmentLimitError(err) {
			t.Errorf("unexpected classification for %v", err)
		}
	}
}

// A caller whose RPC was cancelled while queueing on the rate-limit mutex
// must not burn the API slot. Returning the error is only half of that:
// the one-second pacing sleep runs while the mutex is still held, so a
// cancelled call that still pays it delays every call queued behind it.
// The elapsed time is asserted for exactly that reason -- checking only
// the error value passes whether or not the slot was released promptly.
func TestRateLimitedCallsHonorCancellation(t *testing.T) {
	c, err := NewNcloudCloud("test-access", "test-secret", "KR", "http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()

	if _, err := c.CreateVolume(ctx, &VolumeOptions{Name: "x", SizeGiB: 10, VolumeType: "CB1", ZoneCode: "KR-2"}); !errors.Is(err, context.Canceled) {
		t.Errorf("CreateVolume: expected context.Canceled, got %v", err)
	}
	if err := c.AttachVolume(ctx, "9000001", "12345"); !errors.Is(err, context.Canceled) {
		t.Errorf("AttachVolume: expected context.Canceled, got %v", err)
	}
	if err := c.DetachVolume(ctx, "9000001"); !errors.Is(err, context.Canceled) {
		t.Errorf("DetachVolume: expected context.Canceled, got %v", err)
	}

	// Three cancelled calls that each paid the pacing sleep would take
	// three seconds; releasing the slot immediately takes microseconds.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("three cancelled calls took %v; the pacing sleep is still being paid on the cancelled path", elapsed)
	}
}

// wrapAPIResponse puts a payload in the envelope the Ncloud API uses and
// the vendored SDK expects. The SDK unwraps it by hand rather than with a
// named field, and its index arithmetic needs a separator between the
// envelope key and the payload — hence the spaces, which the real
// gateway's formatting supplies.
func wrapAPIResponse(key, payload string) string {
	return `{ "` + key + `" : ` + payload + ` }`
}

// A server spec lookup that fails is an outage, not "this spec reports no
// attachment limit". Swallowing it would hand the caller a zero limit that
// reads as a definitive answer: the node startup skips its retries and
// blames the spec instead of the gateway.
func TestGetServerInstanceFailsWhenTheSpecLookupFails(t *testing.T) {
	var specCalls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/getServerInstanceDetail"):
			_, _ = io.WriteString(w, wrapAPIResponse("getServerInstanceDetailResponse",
				`{"returnCode":"0","totalRows":1,"serverInstanceList":[{"serverInstanceNo":"200000002","zoneCode":"KR-2","regionCode":"KR","serverSpecCode":"s2-g3"}]}`))
		case strings.HasSuffix(r.URL.Path, "/getServerSpecDetail"):
			atomic.AddInt32(&specCalls, 1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"responseError":{"returnCode":"1000000","returnMessage":"gateway down"}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := NewNcloudCloud("test-access", "test-secret", "KR", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := c.GetServerInstance(context.Background(), "200000002")
	if err == nil {
		t.Fatalf("expected an error, got instance with limit %d", inst.MaxBlockStorageCount)
	}
	if !strings.Contains(err.Error(), "server spec") {
		t.Errorf("error should name the spec lookup: %v", err)
	}
	if got := atomic.LoadInt32(&specCalls); got != 1 {
		t.Errorf("expected one spec lookup, got %d", got)
	}
}

// The instance lookup succeeding with a spec that carries a limit is the
// normal path; the limit reaches the caller.
func TestGetServerInstanceReportsTheSpecLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/getServerInstanceDetail"):
			_, _ = io.WriteString(w, wrapAPIResponse("getServerInstanceDetailResponse",
				`{"returnCode":"0","totalRows":1,"serverInstanceList":[{"serverInstanceNo":"200000002","zoneCode":"KR-2","regionCode":"KR","serverSpecCode":"s2-g3"}]}`))
		case strings.HasSuffix(r.URL.Path, "/getServerSpecDetail"):
			_, _ = io.WriteString(w, wrapAPIResponse("getServerSpecDetailResponse",
				`{"returnCode":"0","serverSpecList":[{"serverSpecCode":"s2-g3","blockStorageMaxCount":20}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := NewNcloudCloud("test-access", "test-secret", "KR", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := c.GetServerInstance(context.Background(), "200000002")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inst.MaxBlockStorageCount != 20 || inst.ZoneCode != "KR-2" {
		t.Errorf("got limit=%d zone=%q, want 20/KR-2", inst.MaxBlockStorageCount, inst.ZoneCode)
	}
}

// The vendored SDK skips unmarshalling a 2xx body that contains
// `{"error"` and returns a nil error with it, so the caller is handed a
// zero-valued response. For a list call that is an empty result, and the
// driver reads an empty result as "this volume does not exist" --
// answering a delete or an unpublish with success, or creating a second
// volume for a name that already exists. The envelope's returnCode is
// what separates the two: the live gateway sets it on every response,
// including a list that is legitimately empty.
func TestUnparsedAPIResponseIsNotAnEmptyResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// 200 with an error body: exactly what makes the SDK skip parsing.
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"error":{"errorCode":"800","message":"gateway rejected the request"}}`)
	}))
	defer srv.Close()

	c, err := NewNcloudCloud("k", "s", "KR", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	_, err = c.GetVolumeByID(ctx, "9000001")
	if !errors.Is(err, errUnparsedAPIResponse) {
		t.Errorf("GetVolumeByID: want errUnparsedAPIResponse, got %v", err)
	}
	if errors.Is(err, ErrVolumeNotFound) {
		t.Error("GetVolumeByID reported the volume as missing; a delete would answer success on it")
	}

	_, err = c.GetVolumeByName(ctx, "pvc-whatever")
	if !errors.Is(err, errUnparsedAPIResponse) {
		t.Errorf("GetVolumeByName: want errUnparsedAPIResponse, got %v", err)
	}
	if errors.Is(err, ErrVolumeNotFound) {
		t.Error("GetVolumeByName reported no such name; CreateVolume would make a duplicate")
	}

	if err := c.DeleteVolume(ctx, "9000001"); !errors.Is(err, errUnparsedAPIResponse) {
		t.Errorf("DeleteVolume: want errUnparsedAPIResponse, got %v", err)
	}
	if err := c.DetachVolume(ctx, "9000001"); !errors.Is(err, errUnparsedAPIResponse) {
		t.Errorf("DetachVolume: want errUnparsedAPIResponse, got %v", err)
	}
}

// A healthy response must not trip the guard, including one whose result
// is genuinely empty -- measured on the live gateway as returnCode "0"
// with totalRows 0.
func TestEmptyListWithReturnCodeIsStillNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, wrapAPIResponse("getBlockStorageInstanceListResponse",
			`{"returnCode":"0","totalRows":0,"blockStorageInstanceList":[]}`))
	}))
	defer srv.Close()

	c, err := NewNcloudCloud("k", "s", "KR", srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.GetVolumeByID(context.Background(), "9000001")
	if !errors.Is(err, ErrVolumeNotFound) {
		t.Errorf("an empty but parsed list must stay ErrVolumeNotFound, got %v", err)
	}
}

// The gateway's error body is `{"responseError": {"returnCode": ...}}`.
// Under a 2xx status the SDK does not reject it: the outer key is
// unwrapped like a success envelope, returnCode comes through carrying
// the error's code and the payload stays empty. Reproduced against the
// vendored decode path (2026-10-06). The nil check above does not see
// this shape, so a code other than "0" has to be an error as well --
// every healthy response, the mutation endpoints included, answers "0".
func TestErrorEnvelopeUnderA2xxIsNotAnEmptyResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, wrapAPIResponse("responseError",
			`{"returnCode":"1101015","returnMessage":"Storage allocation exceeded"}`))
	}))
	defer srv.Close()

	c, err := NewNcloudCloud("k", "s", "KR", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	var apiErr *apiError
	_, err = c.GetVolumeByID(ctx, "9000001")
	if !errors.As(err, &apiErr) || apiErr.code != "1101015" {
		t.Errorf("GetVolumeByID: want an apiError carrying the returnCode, got %v", err)
	}
	if errors.Is(err, ErrVolumeNotFound) {
		t.Error("GetVolumeByID reported the volume as missing; a delete would answer success on it")
	}
	if err != nil && !strings.Contains(err.Error(), "1101015") {
		t.Errorf("the returnCode belongs in the message for diagnosis, got %v", err)
	}

	_, err = c.GetVolumeByName(ctx, "pvc-whatever")
	if err == nil || errors.Is(err, ErrVolumeNotFound) {
		t.Errorf("GetVolumeByName: an error envelope must not read as no such name, got %v", err)
	}

	if err := c.DeleteVolume(ctx, "9000001"); err == nil {
		t.Error("DeleteVolume reported success on an error envelope")
	}
	if err := c.DetachVolume(ctx, "9000001"); err == nil {
		t.Error("DetachVolume reported success on an error envelope")
	}
	if err := c.ResizeVolume(ctx, "9000001", 20); err == nil {
		t.Error("ResizeVolume reported success on an error envelope")
	}

	// The attach-limit code keeps its classification in this shape too, so
	// the controller still answers RESOURCE_EXHAUSTED rather than Internal.
	if err := c.AttachVolume(ctx, "9000001", "9000002"); !errors.Is(err, ErrAttachmentLimitReached) {
		t.Errorf("AttachVolume: want ErrAttachmentLimitReached for returnCode 1101015, got %v", err)
	}
}
