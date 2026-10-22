// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

func rwoMountCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	}
}

func threeSnapshots() []*cloud.Snapshot {
	var snaps []*cloud.Snapshot
	for i := 1; i <= 3; i++ {
		snaps = append(snaps, &cloud.Snapshot{
			ID:             fmt.Sprintf("snap-%d", i),
			Name:           fmt.Sprintf("snapshot-%d", i),
			SourceVolumeID: "9000001",
			SizeBytes:      10 * utils.GiB,
			CreatedAt:      time.Now(),
			ReadyToUse:     true,
		})
	}
	return snaps
}

func TestListSnapshotsBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		req         *csi.ListSnapshotsRequest
		wantCode    codes.Code
		wantEntries int
		wantNext    string
	}{
		{
			name:     "negative max entries returns InvalidArgument",
			req:      &csi.ListSnapshotsRequest{MaxEntries: -1},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "starting token beyond int32 range returns Aborted",
			req:      &csi.ListSnapshotsRequest{StartingToken: "4294967295"},
			wantCode: codes.Aborted,
		},
		{
			name:     "starting token just past int32 max returns Aborted",
			req:      &csi.ListSnapshotsRequest{StartingToken: "2147483648"},
			wantCode: codes.Aborted,
		},
		{
			name:     "starting token beyond list length returns Aborted",
			req:      &csi.ListSnapshotsRequest{StartingToken: "4"},
			wantCode: codes.Aborted,
		},
		{
			name:     "non-numeric starting token returns Aborted",
			req:      &csi.ListSnapshotsRequest{StartingToken: "abc"},
			wantCode: codes.Aborted,
		},
		{
			name:        "starting token equal to list length returns empty page",
			req:         &csi.ListSnapshotsRequest{StartingToken: "3"},
			wantCode:    codes.OK,
			wantEntries: 0,
		},
		{
			name:        "max entries pages the list",
			req:         &csi.ListSnapshotsRequest{MaxEntries: 2},
			wantCode:    codes.OK,
			wantEntries: 2,
			wantNext:    "2",
		},
		{
			name:        "second page returns the remainder",
			req:         &csi.ListSnapshotsRequest{MaxEntries: 2, StartingToken: "2"},
			wantCode:    codes.OK,
			wantEntries: 1,
		},
		{
			name:        "zero max entries returns everything",
			req:         &csi.ListSnapshotsRequest{},
			wantCode:    codes.OK,
			wantEntries: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &cloud.MockCloud{
				ListSnapshotsFn: func(_ context.Context) ([]*cloud.Snapshot, error) {
					return threeSnapshots(), nil
				},
			}
			d := newTestDriver(mc)

			resp, err := d.ListSnapshots(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tt.wantCode)
				}
				st, _ := status.FromError(err)
				if st.Code() != tt.wantCode {
					t.Errorf("expected code %v, got %v: %s", tt.wantCode, st.Code(), st.Message())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(resp.Entries) != tt.wantEntries {
				t.Errorf("expected %d entries, got %d", tt.wantEntries, len(resp.Entries))
			}
			if resp.NextToken != tt.wantNext {
				t.Errorf("expected next token %q, got %q", tt.wantNext, resp.NextToken)
			}
		})
	}
}

func TestUnaryServerInterceptorRecoversPanic(t *testing.T) {
	handler := func(_ context.Context, _ interface{}) (interface{}, error) {
		panic("boom")
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/ListSnapshots"}

	resp, err := unaryServerInterceptor(context.Background(), nil, info, handler)
	if resp != nil {
		t.Errorf("expected nil response after panic, got %v", resp)
	}
	if err == nil {
		t.Fatal("expected error after panic, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Errorf("expected Internal status, got %v", err)
	}
}

func TestCreateVolumeContentSource(t *testing.T) {
	newRequest := func(source *csi.VolumeContentSource) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:                "test-vol",
			VolumeCapabilities:  []*csi.VolumeCapability{rwoMountCapability()},
			CapacityRange:       &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
			VolumeContentSource: source,
		}
	}

	tests := []struct {
		name       string
		req        *csi.CreateVolumeRequest
		setupCloud func(*cloud.MockCloud)
		wantCode   codes.Code
	}{
		{
			name: "volume clone source returns InvalidArgument",
			req: newRequest(&csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Volume{
					Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "9000011"},
				},
			}),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "snapshot source without ID returns InvalidArgument",
			req: newRequest(&csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{},
				},
			}),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing source snapshot returns NotFound",
			req: newRequest(&csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-missing"},
				},
			}),
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.GetSnapshotByIDFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return nil, cloud.ErrSnapshotNotFound
				}
			},
			wantCode: codes.NotFound,
		},
		{
			name: "restore smaller than snapshot returns OutOfRange",
			req: newRequest(&csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-big"},
				},
			}),
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.GetSnapshotByIDFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{ID: "snap-big", SourceVolumeID: "9000010", SizeBytes: 30 * utils.GiB}, nil
				}
			},
			wantCode: codes.OutOfRange,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &cloud.MockCloud{}
			if tt.setupCloud != nil {
				tt.setupCloud(mc)
			}
			d := newTestDriver(mc)

			_, err := d.CreateVolume(context.Background(), tt.req)
			if err == nil {
				t.Fatalf("expected error with code %v, got nil", tt.wantCode)
			}
			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v: %s", tt.wantCode, st.Code(), st.Message())
			}
		})
	}
}

func TestControllerPublishVolumeErrorCodes(t *testing.T) {
	tests := []struct {
		name       string
		req        *csi.ControllerPublishVolumeRequest
		setupCloud func(*cloud.MockCloud)
		wantCode   codes.Code
	}{
		{
			name: "missing volume returns NotFound",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId:         "9404001",
				NodeId:           "node-1",
				VolumeCapability: rwoMountCapability(),
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.NotFound,
		},
		{
			name: "attachment limit returns ResourceExhausted",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId:         "9000001",
				NodeId:           "node-1",
				VolumeCapability: rwoMountCapability(),
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "9000001", Name: "test-vol", SizeBytes: 10 * utils.GiB,
						StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal, VolumeTypeCode: "CB1",
					}, nil
				}
				// Publish waits for the volume to finish being created
				// before attaching; this one already reports StatusCreated.
				mc.WaitForVolumeStatusFn = func(_ context.Context, _ string, _ []string) error {
					return nil
				}
				mc.AttachVolumeFn = func(_ context.Context, _, _ string) error {
					return fmt.Errorf("server %q already has 20 of 20 attachments: %w", "node-1", cloud.ErrAttachmentLimitReached)
				}
			},
			wantCode: codes.ResourceExhausted,
		},
		{
			name: "readonly request returns InvalidArgument",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId:         "9000001",
				NodeId:           "node-1",
				VolumeCapability: rwoMountCapability(),
				Readonly:         true,
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "unsupported access mode returns InvalidArgument",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId: "9000001",
				NodeId:   "node-1",
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				},
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &cloud.MockCloud{}
			if tt.setupCloud != nil {
				tt.setupCloud(mc)
			}
			d := newTestDriver(mc)

			_, err := d.ControllerPublishVolume(context.Background(), tt.req)
			if err == nil {
				t.Fatalf("expected error with code %v, got nil", tt.wantCode)
			}
			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v: %s", tt.wantCode, st.Code(), st.Message())
			}
		})
	}
}

func TestStopBeforeRunDoesNotPanic(t *testing.T) {
	d := &Driver{}
	d.Stop() // must be a no-op, not a nil dereference
}

func TestControllerUnpublishVolume(t *testing.T) {
	attachedVolume := func() *cloud.Volume {
		return &cloud.Volume{
			ID: "9000001", Name: "test-vol", ServerInstanceNo: "node-1",
			StatusCode: cloud.StatusAttached, OperationCode: cloud.OpNormal,
		}
	}
	detachedVolume := func() *cloud.Volume {
		return &cloud.Volume{
			ID: "9000001", Name: "test-vol", ServerInstanceNo: "",
			StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal,
		}
	}

	tests := []struct {
		name       string
		req        *csi.ControllerUnpublishVolumeRequest
		volume     *cloud.Volume
		wantDetach bool
	}{
		{
			name:       "empty node ID detaches an attached volume",
			req:        &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001"},
			volume:     attachedVolume(),
			wantDetach: true,
		},
		{
			name:       "empty node ID succeeds for a detached volume without detaching",
			req:        &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001"},
			volume:     detachedVolume(),
			wantDetach: false,
		},
		{
			name:       "matching node ID detaches",
			req:        &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001", NodeId: "node-1"},
			volume:     attachedVolume(),
			wantDetach: true,
		},
		{
			name:       "different explicit node ID succeeds without detaching",
			req:        &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001", NodeId: "node-2"},
			volume:     attachedVolume(),
			wantDetach: false,
		},
		{
			// The idempotent no-op for a non-matching node must not depend
			// on the volume's operation state.
			name: "different node ID succeeds even while an operation is in progress",
			req:  &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001", NodeId: "node-2"},
			volume: &cloud.Volume{
				ID: "9000001", Name: "test-vol", ServerInstanceNo: "node-1",
				StatusCode: cloud.StatusAttached, OperationCode: "DETAC",
			},
			wantDetach: false,
		},
		{
			// An already-detached volume satisfies unpublish even while some
			// other operation (e.g. a snapshot) is in progress: the operation
			// guard only applies when a detach would actually be issued.
			name: "detached volume with an operation in progress still succeeds",
			req:  &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001"},
			volume: &cloud.Volume{
				ID: "9000001", Name: "test-vol", ServerInstanceNo: "",
				StatusCode: cloud.StatusCreated, OperationCode: "CHNG",
			},
			wantDetach: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detachCalled := false
			mc := &cloud.MockCloud{
				GetVolumeByIDFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
					return tt.volume, nil
				},
				EnsureDetachVolumeFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
					detachCalled = true
					return detachedVolume(), nil
				},
			}
			d := newTestDriver(mc)

			_, err := d.ControllerUnpublishVolume(context.Background(), tt.req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if detachCalled != tt.wantDetach {
				t.Errorf("expected detach called=%v, got %v", tt.wantDetach, detachCalled)
			}
		})
	}
}

// Reporting success for an unpublish is the one answer that cannot be
// taken back: external-attacher drops the VolumeAttachment finalizer on
// it. While an attach is in flight the volume reads StatusCreated with
// operation ATTAC and serverInstanceNo already set (measured on the live
// API, a ~27 second window), so a status-only check calls that "not
// attached" and succeeds -- the attach then completes with no attachment
// object left to ever detach it.
func TestControllerUnpublishAbortsWhileAnAttachIsInFlight(t *testing.T) {
	attaching := func() *cloud.Volume {
		return &cloud.Volume{
			ID: "9000001", Name: "test-vol", ServerInstanceNo: "node-1",
			StatusCode: cloud.StatusCreated, OperationCode: cloud.OpAttach,
		}
	}

	for _, tt := range []struct {
		name     string
		req      *csi.ControllerUnpublishVolumeRequest
		wantCode codes.Code
	}{
		{
			name:     "matching node ID",
			req:      &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001", NodeId: "node-1"},
			wantCode: codes.Aborted,
		},
		{
			// An empty node ID means "whatever node holds it", which is
			// the node being attached to.
			name:     "empty node ID",
			req:      &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001"},
			wantCode: codes.Aborted,
		},
		{
			// The idempotent no-op for another node stays a no-op: that
			// node does not hold the volume and never will from this
			// attach, so there is nothing to wait for.
			name:     "different node ID still succeeds",
			req:      &csi.ControllerUnpublishVolumeRequest{VolumeId: "9000001", NodeId: "node-2"},
			wantCode: codes.OK,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			detachCalled := false
			mc := &cloud.MockCloud{
				GetVolumeByIDFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
					return attaching(), nil
				},
				EnsureDetachVolumeFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
					detachCalled = true
					return nil, nil
				},
			}

			_, err := newTestDriver(mc).ControllerUnpublishVolume(context.Background(), tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("expected code %v, got %v (%v)", tt.wantCode, got, err)
			}
			if detachCalled {
				t.Error("detach must not be issued while the attach is still running")
			}
		})
	}
}

// CreateVolume returns as soon as the create API answers, so the first
// publish can arrive while the volume is still building -- and the API
// refuses an attach then (HTTP 400, returnCode 1103026, measured).
// Waiting costs one poll; not waiting costs a failed attach, the node
// lookup the error classification needs, and the attacher's backoff.
func TestControllerPublishWaitsForTheVolumeToFinishCreating(t *testing.T) {
	volume := func() *cloud.Volume {
		return &cloud.Volume{
			ID: "9000001", Name: "test-vol", SizeBytes: 10 * utils.GiB,
			StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal, VolumeTypeCode: "CB1",
		}
	}
	req := &csi.ControllerPublishVolumeRequest{
		VolumeId: "9000001", NodeId: "node-1", VolumeCapability: rwoMountCapability(),
	}

	t.Run("waits for StatusCreated before attaching", func(t *testing.T) {
		var order []string
		var waited []string
		attached := false
		mc := &cloud.MockCloud{
			GetVolumeByIDFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
				v := volume()
				if attached {
					v.ServerInstanceNo = "node-1"
					v.StatusCode = cloud.StatusAttached
				}
				return v, nil
			},
			WaitForVolumeStatusFn: func(_ context.Context, _ string, statuses []string) error {
				if len(order) == 0 {
					waited = statuses
				}
				order = append(order, "wait")
				return nil
			},
			AttachVolumeFn: func(_ context.Context, _, _ string) error {
				order = append(order, "attach")
				attached = true
				return nil
			},
		}
		if _, err := newTestDriver(mc).ControllerPublishVolume(context.Background(), req); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(order) < 2 || order[0] != "wait" || order[1] != "attach" {
			t.Errorf("expected a wait before the attach, got %v", order)
		}
		if len(waited) != 1 || waited[0] != cloud.StatusCreated {
			t.Errorf("expected the wait to be for %q, got %v", cloud.StatusCreated, waited)
		}
	})

	t.Run("a volume that never finishes is Aborted, not attached", func(t *testing.T) {
		attachCalled := false
		mc := &cloud.MockCloud{
			GetVolumeByIDFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
				return volume(), nil
			},
			WaitForVolumeStatusFn: func(_ context.Context, volumeID string, _ []string) error {
				return fmt.Errorf("timeout waiting for volume status: %s", volumeID)
			},
			AttachVolumeFn: func(_ context.Context, _, _ string) error {
				attachCalled = true
				return nil
			},
		}
		_, err := newTestDriver(mc).ControllerPublishVolume(context.Background(), req)
		if got := status.Code(err); got != codes.Aborted {
			t.Errorf("expected Aborted, got %v (%v)", got, err)
		}
		if attachCalled {
			t.Error("attach must not be issued for a volume that is not ready")
		}
	})

	t.Run("a volume that disappears while waiting is NotFound", func(t *testing.T) {
		mc := &cloud.MockCloud{
			GetVolumeByIDFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
				return volume(), nil
			},
			WaitForVolumeStatusFn: func(_ context.Context, volumeID string, _ []string) error {
				return fmt.Errorf("volume %s not found: %w", volumeID, cloud.ErrVolumeNotFound)
			},
		}
		_, err := newTestDriver(mc).ControllerPublishVolume(context.Background(), req)
		if got := status.Code(err); got != codes.NotFound {
			t.Errorf("expected NotFound, got %v (%v)", got, err)
		}
	})
}

// A volume ID that cannot be an Ncloud instance number is short-circuited
// before any cloud call: delete/unpublish treat it as already gone, the
// rest answer NotFound. The nil-fn MockCloud panics on any API call, which
// pins the short-circuit.
func TestControllerRejectsMalformedVolumeIDs(t *testing.T) {
	d := newTestDriver(&cloud.MockCloud{})
	ctx := context.Background()
	badID := "not-a-number"
	mountCap := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	}

	if _, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: badID}); err != nil {
		t.Errorf("DeleteVolume: expected success for malformed ID, got %v", err)
	}
	if _, err := d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: badID}); err != nil {
		t.Errorf("ControllerUnpublishVolume: expected success for malformed ID, got %v", err)
	}

	notFoundCalls := map[string]func() error{
		"ControllerPublishVolume": func() error {
			_, err := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
				VolumeId: badID, NodeId: "12345", VolumeCapability: mountCap,
			})
			return err
		},
		"ControllerExpandVolume": func() error {
			_, err := d.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
				VolumeId: badID, CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
			})
			return err
		},
		"ValidateVolumeCapabilities": func() error {
			_, err := d.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: badID, VolumeCapabilities: []*csi.VolumeCapability{mountCap},
			})
			return err
		},
		"CreateSnapshot": func() error {
			_, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap", SourceVolumeId: badID})
			return err
		},
	}
	for name, call := range notFoundCalls {
		err := call()
		if err == nil {
			t.Errorf("%s: expected NotFound for malformed ID, got success", name)
			continue
		}
		if st, _ := status.FromError(err); st.Code() != codes.NotFound {
			t.Errorf("%s: expected NotFound, got %v", name, err)
		}
	}
}

// A volume that disappears while DeleteVolume waits out the detach is
// deleted as far as the CO is concerned.
func TestDeleteVolumeDisappearsDuringDetachWait(t *testing.T) {
	mc := &cloud.MockCloud{
		GetVolumeByIDFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
			return &cloud.Volume{ID: "9000031", StatusCode: "ATTAC", ServerInstanceNo: "12345"}, nil
		},
		EnsureDetachVolumeFn: func(_ context.Context, _ string) (*cloud.Volume, error) { return nil, nil },
		WaitForVolumeStatusFn: func(_ context.Context, volumeID string, _ []string) error {
			return fmt.Errorf("volume %s not found: %w", volumeID, cloud.ErrVolumeNotFound)
		},
	}
	if _, err := newTestDriver(mc).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "9000031"}); err != nil {
		t.Fatalf("expected success when the volume disappears during the detach wait, got %v", err)
	}
}

// Snapshot RPCs hold the same malformed-ID contract as the volume RPCs:
// delete treats it as already gone, list answers empty, and the nil-fn
// MockCloud panics if any API call slips through.
func TestSnapshotRPCsRejectMalformedIDs(t *testing.T) {
	d := newTestDriver(&cloud.MockCloud{})
	ctx := context.Background()

	if _, err := d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "not-a-number"}); err != nil {
		t.Errorf("DeleteSnapshot: expected success for malformed ID, got %v", err)
	}
	for name, req := range map[string]*csi.ListSnapshotsRequest{
		"malformed snapshot_id":      {SnapshotId: "not-a-number"},
		"malformed source_volume_id": {SourceVolumeId: "not-a-number"},
	} {
		resp, err := d.ListSnapshots(ctx, req)
		if err != nil || len(resp.Entries) != 0 {
			t.Errorf("ListSnapshots(%s): expected empty success, got entries=%v err=%v", name, resp.GetEntries(), err)
		}
	}
}

// When both list filters are set, the entry must satisfy both.
func TestListSnapshotsAppliesBothFilters(t *testing.T) {
	mc := &cloud.MockCloud{GetSnapshotByIDFn: func(_ context.Context, id string) (*cloud.Snapshot, error) {
		return &cloud.Snapshot{ID: id, SourceVolumeID: "9000001"}, nil
	}}
	resp, err := newTestDriver(mc).ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SnapshotId: "9000777", SourceVolumeId: "9000002",
	})
	if err != nil || len(resp.Entries) != 0 {
		t.Fatalf("expected empty success for a source mismatch, got entries=%v err=%v", resp.GetEntries(), err)
	}
}

// An idempotent CreateVolume retry that claims a snapshot source must
// validate the snapshot like the fresh-create path does.
func TestCreateVolumeRetryValidatesContentSource(t *testing.T) {
	mc := &cloud.MockCloud{
		GetVolumeByNameFn: func(_ context.Context, name string) (*cloud.Volume, error) {
			return &cloud.Volume{ID: "9000050", Name: name, SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-2"}, nil
		},
		GetSnapshotByIDFn: func(_ context.Context, _ string) (*cloud.Snapshot, error) {
			return nil, cloud.ErrSnapshotNotFound
		},
	}
	_, err := newTestDriver(mc).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "retry-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "9000404"},
		}},
	})
	if err == nil {
		t.Fatal("expected NotFound for a vanished source snapshot, got success")
	}
	if st, _ := status.FromError(err); st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

// A volume without a reported zone must not get an empty-string zone
// segment: that nodeAffinity matches no node and the PV pends forever.
func TestCreateVolumeOmitsTopologyWithoutZone(t *testing.T) {
	mc := &cloud.MockCloud{
		GetVolumeByNameFn: func(_ context.Context, _ string) (*cloud.Volume, error) {
			return nil, cloud.ErrVolumeNotFound
		},
		CreateVolumeFn: func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
			return &cloud.Volume{ID: "9000060", Name: opts.Name, SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1"}, nil
		},
		GetVolumeByIDFn: func(_ context.Context, id string) (*cloud.Volume, error) {
			return &cloud.Volume{ID: id, Name: "zoneless", SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: ""}, nil
		},
	}
	resp, err := newTestDriver(mc).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:          "zoneless",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := len(resp.Volume.AccessibleTopology); n != 0 {
		t.Errorf("expected no topology for a zoneless volume, got %v", resp.Volume.AccessibleTopology)
	}
}

// A volume held by another node is not ours to take: the CO releases an
// attachment with ControllerUnpublishVolume. The nil-fn MockCloud panics
// on any call beyond the lookup, which pins that no detach is attempted.
func TestControllerPublishVolumeRefusesToStealFromAnotherNode(t *testing.T) {
	mc := &cloud.MockCloud{GetVolumeByIDFn: func(_ context.Context, id string) (*cloud.Volume, error) {
		return &cloud.Volume{ID: id, Name: "held", ServerInstanceNo: "node-1", StatusCode: "ATTAC", OperationCode: "NULL"}, nil
	}}
	_, err := newTestDriver(mc).ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "9000001",
		NodeId:   "node-2",
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		},
	})
	if err == nil {
		t.Fatal("expected FailedPrecondition for a volume attached elsewhere, got success")
	}
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", err)
	}
}

// StorageClass parameters the driver does not understand are rejected
// rather than silently ignored; the keys Kubernetes owns pass through.
func TestCreateVolumeRejectsUnknownParameters(t *testing.T) {
	newReq := func(params map[string]string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:          "params-vol",
			CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
			VolumeCapabilities: []*csi.VolumeCapability{{
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			}},
			Parameters: params,
		}
	}

	// Rejected before any cloud call — the nil-fn mock would panic.
	for name, params := range map[string]map[string]string{
		"typo":                     {"tpye": "CB1"},
		"unsupported key":          {"type": "CB1", "encrypted": "true"},
		"conflicting type entries": {"type": "CB1", "Type": "FB1"},
	} {
		_, err := newTestDriver(&cloud.MockCloud{}).CreateVolume(context.Background(), newReq(params))
		if err == nil {
			t.Errorf("%s: expected InvalidArgument, got success", name)
			continue
		}
		if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}

	// Keys owned by Kubernetes must pass through untouched: the prefixed
	// namespace, the deprecated camel-case secret spellings, and the bare
	// "fstype" the provisioner still reads (case insensitively).
	mc := &cloud.MockCloud{
		GetVolumeByNameFn: func(_ context.Context, _ string) (*cloud.Volume, error) { return nil, cloud.ErrVolumeNotFound },
		CreateVolumeFn: func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
			if opts.VolumeType != "FB1" {
				t.Errorf("expected volume type FB1, got %q", opts.VolumeType)
			}
			return &cloud.Volume{ID: "9000070", Name: opts.Name, SizeBytes: 100 * utils.GiB, VolumeTypeCode: opts.VolumeType, ZoneCode: "KR-2"}, nil
		},
		GetVolumeByIDFn: func(_ context.Context, id string) (*cloud.Volume, error) {
			return &cloud.Volume{ID: id, Name: "params-vol", SizeBytes: 100 * utils.GiB, VolumeTypeCode: "FB1", ZoneCode: "KR-2"}, nil
		},
	}
	req := newReq(map[string]string{
		"type":                             "FB1",
		"fstype":                           "xfs",
		"FsType":                           "xfs",
		"csiProvisionerSecretName":         "legacy-secret",
		"csi.storage.k8s.io/fstype":        "xfs",
		"csi.storage.k8s.io/pvc/name":      "data",
		"csi.storage.k8s.io/pvc/namespace": "default",
		"csi.storage.k8s.io/pv/name":       "pvc-abc",
	})
	req.CapacityRange = &csi.CapacityRange{RequiredBytes: 100 * utils.GiB}
	if _, err := newTestDriver(mc).CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("expected success with CO-owned parameters, got %v", err)
	}
}

// The CO namespace is matched exactly. A key that merely starts with "csi"
// (csitype), or a prefixed or deprecated key in the wrong case, is one the
// provisioner does not recognize either -- so it is rejected like any
// other unknown key instead of being ignored twice.
func TestCreateVolumeRejectsKeysThatOnlyLookLikeTheCONamespace(t *testing.T) {
	for name, params := range map[string]map[string]string{
		"csi prefix typo":           {"csitype": "FB1"},
		"deprecated key wrong case": {"CsiProvisionerSecretName": "s"},
		"prefix wrong case":         {"CSI.storage.k8s.io/fstype": "xfs"},
	} {
		_, err := newTestDriver(&cloud.MockCloud{}).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "lookalike",
			CapacityRange:      &csi.CapacityRange{RequiredBytes: 100 * utils.GiB},
			VolumeCapabilities: []*csi.VolumeCapability{rwoMountCapability()},
			Parameters:         params,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
}

// The type key is matched case insensitively: a "Type:" that used to
// provision the default type silently now provisions what it asked for.
func TestCreateVolumeAcceptsTypeKeyInAnyCase(t *testing.T) {
	for _, key := range []string{"type", "Type", "TYPE"} {
		var got string
		mc := &cloud.MockCloud{
			GetVolumeByNameFn: func(_ context.Context, _ string) (*cloud.Volume, error) { return nil, cloud.ErrVolumeNotFound },
			CreateVolumeFn: func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
				got = opts.VolumeType
				return &cloud.Volume{ID: "9000080", Name: opts.Name, SizeBytes: 100 * utils.GiB, VolumeTypeCode: opts.VolumeType, ZoneCode: "KR-2"}, nil
			},
			GetVolumeByIDFn: func(_ context.Context, id string) (*cloud.Volume, error) {
				return &cloud.Volume{ID: id, Name: "cased", SizeBytes: 100 * utils.GiB, VolumeTypeCode: "FB1", ZoneCode: "KR-2"}, nil
			},
		}
		_, err := newTestDriver(mc).CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:          "cased",
			CapacityRange: &csi.CapacityRange{RequiredBytes: 100 * utils.GiB},
			VolumeCapabilities: []*csi.VolumeCapability{{
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			}},
			Parameters: map[string]string{key: "fb1"},
		})
		if err != nil {
			t.Errorf("%q: unexpected error: %v", key, err)
			continue
		}
		if got != "FB1" {
			t.Errorf("%q: expected volume type FB1, got %q", key, got)
		}
	}
}
