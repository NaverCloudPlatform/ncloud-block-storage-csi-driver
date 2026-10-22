// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

// ControllerPublishVolume waits for a volume to finish creating and then
// for the attach to settle, which can outlast the attacher's 60-second
// deadline. The attacher then moves on, and if the VolumeAttachment is
// deleted it calls ControllerUnpublishVolume while the publish is still
// running: before the API shows the attach, that unpublish reads "not
// attached" and succeeds, and the attach completes with nothing left to
// detach it. The guard refuses the second caller until the first returns.
func TestControllerRPCsRefuseAConcurrentRequestForTheSameVolume(t *testing.T) {
	const volumeID = "9000001"

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	attached := false

	mc := &cloud.MockCloud{
		GetVolumeByIDFn: func(_ context.Context, id string) (*cloud.Volume, error) {
			if id != volumeID {
				return nil, cloud.ErrVolumeNotFound
			}
			mu.Lock()
			defer mu.Unlock()
			v := &cloud.Volume{ID: volumeID, Name: "pvc-a", SizeBytes: 10 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-1",
				StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal}
			if attached {
				v.StatusCode, v.ServerInstanceNo = cloud.StatusAttached, "node-1"
			}
			return v, nil
		},
		WaitForVolumeStatusFn: func(context.Context, string, []string) error { return nil },
		AttachVolumeFn: func(context.Context, string, string) error {
			// Parked here: the attach HTTP call is in flight and the API
			// does not show the volume as attached yet.
			once.Do(func() { close(started) })
			<-release
			mu.Lock()
			attached = true
			mu.Unlock()
			return nil
		},
		DetachVolumeFn: func(context.Context, string) error { return nil },
		EnsureDetachVolumeFn: func(_ context.Context, id string) (*cloud.Volume, error) {
			return &cloud.Volume{ID: id, StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal}, nil
		},
		DeleteVolumeFn:        func(context.Context, string) error { return nil },
		WaitForVolumeResizeFn: func(context.Context, string, int64) error { return nil },
		ResizeVolumeFn:        func(context.Context, string, int32) error { return nil },
	}
	d := newTestDriver(mc)

	first := make(chan error, 1)
	go func() {
		_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: volumeID, NodeId: "node-1", VolumeCapability: rwoMountCapability(),
		})
		first <- err
	}()
	<-started

	// Same volume, any controller RPC: refused while the publish holds it.
	if _, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID,
	}); status.Code(err) != codes.Aborted {
		t.Errorf("unpublish beside an in-flight publish: want Aborted, got %v", err)
	}
	if _, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: volumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
	}); status.Code(err) != codes.Aborted {
		t.Errorf("expand beside an in-flight publish: want Aborted, got %v", err)
	}
	if _, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: volumeID,
	}); status.Code(err) != codes.Aborted {
		t.Errorf("delete beside an in-flight publish: want Aborted, got %v", err)
	}
	if _, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID, NodeId: "node-1", VolumeCapability: rwoMountCapability(),
	}); status.Code(err) != codes.Aborted {
		t.Errorf("a retried publish beside the first: want Aborted, got %v", err)
	}

	// A different volume is unaffected (the mock reports it as deleted,
	// which unpublish answers with success).
	if _, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "9000002",
	}); err != nil {
		t.Errorf("a different volume must not be blocked: %v", err)
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf("the parked publish failed: %v", err)
	}

	// Released on return: the unpublish now runs against an attached
	// volume and detaches it.
	if _, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volumeID, NodeId: "node-1",
	}); err != nil {
		t.Errorf("the volume was not released when the publish returned: %v", err)
	}
}

// CreateVolume's lookup-then-create pair is guarded by the Ncloud name,
// which is what the API enforces uniqueness on; requests for other names
// and operations on volume IDs are not held up by it.
func TestCreateVolumeGuardIsKeyedByName(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	mc := &cloud.MockCloud{
		GetVolumeByNameFn: func(context.Context, string) (*cloud.Volume, error) { return nil, cloud.ErrVolumeNotFound },
		CreateVolumeFn: func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
			if opts.Name == "pvc-held" {
				once.Do(func() { close(started) })
				<-release
			}
			return &cloud.Volume{ID: "9" + opts.Name[len(opts.Name)-6:], Name: opts.Name, SizeBytes: 10 * utils.GiB,
				VolumeTypeCode: "CB1", ZoneCode: "KR-1", StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal}, nil
		},
		GetVolumeByIDFn: func(_ context.Context, id string) (*cloud.Volume, error) {
			return &cloud.Volume{ID: id, SizeBytes: 10 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-1",
				StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal}, nil
		},
	}
	d := newTestDriver(mc)

	first := make(chan error, 1)
	go func() {
		_, err := d.CreateVolume(context.Background(), finalityCreateRequest("pvc-held"))
		first <- err
	}()
	<-started

	if _, err := d.CreateVolume(context.Background(), finalityCreateRequest("pvc-held")); status.Code(err) != codes.Aborted {
		t.Errorf("a second CreateVolume for the held name: want Aborted, got %v", err)
	}
	if _, err := d.CreateVolume(context.Background(), finalityCreateRequest("pvc-other")); err != nil {
		t.Errorf("a CreateVolume for another name must not be blocked: %v", err)
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf("the parked create failed: %v", err)
	}
	if _, err := d.CreateVolume(context.Background(), finalityCreateRequest("pvc-held")); err != nil {
		t.Errorf("the name was not released when the create returned: %v", err)
	}
}

// An attach that waited its turn behind other attaches until the RPC's
// deadline passed is answered with the context's code at once. The node
// lookup that classifies a real attach failure would otherwise spend two
// more API calls after the deadline -- on the slow-API day that made the
// queue long in the first place -- and answer Internal.
func TestControllerPublishStopsAtTheDeadlineWithoutTheNodeLookup(t *testing.T) {
	for _, tt := range []struct {
		name     string
		ctx      func() (context.Context, context.CancelFunc)
		wantCode codes.Code
	}{
		{"deadline", func() (context.Context, context.CancelFunc) { return context.WithTimeout(context.Background(), 0) }, codes.DeadlineExceeded},
		{"cancellation", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}, codes.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var lookups int32
			mc := &cloud.MockCloud{
				GetVolumeByIDFn: func(context.Context, string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", SizeBytes: 10 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-1",
						StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal}, nil
				},
				WaitForVolumeStatusFn: func(context.Context, string, []string) error { return nil },
				// What cloud.AttachVolume does when the deadline passed
				// while the call was queued on the rate-limit mutex.
				AttachVolumeFn: func(ctx context.Context, _, _ string) error { return ctx.Err() },
				GetServerInstanceFn: func(context.Context, string) (*cloud.ServerInstance, error) {
					atomic.AddInt32(&lookups, 1)
					return nil, cloud.ErrServerInstanceNotFound
				},
			}
			d := newTestDriver(mc)

			ctx, cancel := tt.ctx()
			defer cancel()
			_, err := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
				VolumeId: "9000001", NodeId: "node-1", VolumeCapability: rwoMountCapability(),
			})
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if n := atomic.LoadInt32(&lookups); n != 0 {
				t.Errorf("the node lookup ran %d time(s) after the deadline", n)
			}
		})
	}
}

// Only an attach that was never issued is answered with the context's
// code. One that went out and failed after the deadline passed -- the API
// refusing it, or the HTTP client timing out, which also unwraps to
// DeadlineExceeded -- keeps its own classification.
func TestControllerPublishClassifiesAnIssuedAttachEvenAfterTheDeadline(t *testing.T) {
	for _, tt := range []struct {
		name        string
		attachErr   error
		wantCode    codes.Code
		wantLookups int32
	}{
		{"attachment limit", fmt.Errorf("attach: %w", cloud.ErrAttachmentLimitReached), codes.ResourceExhausted, 0},
		{"HTTP client timeout", &url.Error{Op: "Post", URL: "https://api.invalid/", Err: context.DeadlineExceeded}, codes.NotFound, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var lookups int32
			mc := &cloud.MockCloud{
				GetVolumeByIDFn: func(context.Context, string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", SizeBytes: 10 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-1",
						StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal}, nil
				},
				WaitForVolumeStatusFn: func(context.Context, string, []string) error { return nil },
				AttachVolumeFn:        func(context.Context, string, string) error { return tt.attachErr },
				GetServerInstanceFn: func(context.Context, string) (*cloud.ServerInstance, error) {
					atomic.AddInt32(&lookups, 1)
					return nil, cloud.ErrServerInstanceNotFound
				},
			}
			d := newTestDriver(mc)
			ctx, cancel := context.WithTimeout(context.Background(), 0)
			defer cancel()
			_, err := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
				VolumeId: "9000001", NodeId: "node-1", VolumeCapability: rwoMountCapability(),
			})
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if n := atomic.LoadInt32(&lookups); n != tt.wantLookups {
				t.Errorf("node lookups = %d, want %d", n, tt.wantLookups)
			}
		})
	}
}
