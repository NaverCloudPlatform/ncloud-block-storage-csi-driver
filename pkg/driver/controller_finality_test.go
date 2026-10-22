// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

// The provisioner (external-provisioner v6.1.0, checkError) and the
// snapshotter (external-snapshotter v8.4.0, isCSIFinalError) treat every
// code except Canceled, DeadlineExceeded, Unavailable, ResourceExhausted
// and Aborted as final: the request leaves their in-progress set, and if
// the PVC or VolumeSnapshot is deleted before the next retry nothing ever
// comes back for the resource the backend may have created. These tests
// pin which failures of a create are allowed to be final.

var (
	// What the SDK hands back when the HTTP client gives up: the request
	// may have been accepted by the backend.
	transportTimeout = &url.Error{Op: "Post", URL: "https://api.invalid/vserver/v2/createBlockStorageInstance", Err: context.DeadlineExceeded}
	// What the SDK hands back when the API refuses the request outright.
	apiRejection = fmt.Errorf("failed to create block storage: %w",
		errors.New(`Status: 400 Bad Request, Body: {"responseError": {"returnCode": "1000020", "returnMessage": "Invalid parameter"}}`))
)

func finalityCreateRequest(name string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:               name,
		VolumeCapabilities: []*csi.VolumeCapability{rwoMountCapability()},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
	}
}

func TestCreateVolumeErrorFinality(t *testing.T) {
	created := func() *cloud.Volume {
		return &cloud.Volume{
			ID: "9000001", Name: "pvc-a", SizeBytes: 10 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-1",
			StatusCode: cloud.StatusCreated, OperationCode: cloud.OpNormal,
		}
	}
	notFound := func(context.Context, string) (*cloud.Volume, error) { return nil, cloud.ErrVolumeNotFound }

	for _, tt := range []struct {
		name     string
		cloud    func() *cloud.MockCloud
		wantCode codes.Code
		wantID   string
	}{
		{
			name: "the name lookup fails: a read says nothing about existence",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetVolumeByNameFn: func(context.Context, string) (*cloud.Volume, error) { return nil, transportTimeout },
				}
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "the create times out on the wire: the backend may have created it",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetVolumeByNameFn: notFound,
					CreateVolumeFn: func(context.Context, *cloud.VolumeOptions) (*cloud.Volume, error) {
						return nil, fmt.Errorf("failed to create block storage: %w", transportTimeout)
					},
				}
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "the API rejects the request: nothing was created, so final",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetVolumeByNameFn: notFound,
					CreateVolumeFn:    func(context.Context, *cloud.VolumeOptions) (*cloud.Volume, error) { return nil, apiRejection },
				}
			},
			wantCode: codes.Internal,
		},
		{
			name: "the name is in use: the volume behind it is returned",
			cloud: func() *cloud.MockCloud {
				var lookups int32
				return &cloud.MockCloud{
					GetVolumeByNameFn: func(context.Context, string) (*cloud.Volume, error) {
						// Not there for the first lookup, there once the
						// create has collided with it.
						if atomic.AddInt32(&lookups, 1) == 1 {
							return nil, cloud.ErrVolumeNotFound
						}
						return created(), nil
					},
					CreateVolumeFn: func(context.Context, *cloud.VolumeOptions) (*cloud.Volume, error) {
						return nil, fmt.Errorf("create block storage: %w", cloud.ErrNameInUse)
					},
				}
			},
			wantCode: codes.OK,
			wantID:   "9000001",
		},
		{
			name: "the name is in use but the volume is not readable yet: pending, not final",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetVolumeByNameFn: notFound,
					CreateVolumeFn: func(context.Context, *cloud.VolumeOptions) (*cloud.Volume, error) {
						return nil, fmt.Errorf("create block storage: %w", cloud.ErrNameInUse)
					},
				}
			},
			wantCode: codes.Aborted,
		},
		{
			name: "created, but the re-read fails: the create response stands in",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetVolumeByNameFn: notFound,
					CreateVolumeFn:    func(context.Context, *cloud.VolumeOptions) (*cloud.Volume, error) { return created(), nil },
					GetVolumeByIDFn:   func(context.Context, string) (*cloud.Volume, error) { return nil, transportTimeout },
				}
			},
			wantCode: codes.OK,
			wantID:   "9000001",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDriver(tt.cloud())
			resp, err := d.CreateVolume(context.Background(), finalityCreateRequest("pvc-a"))
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if tt.wantID != "" {
				if resp.GetVolume().GetVolumeId() != tt.wantID {
					t.Errorf("volume ID = %q, want %q", resp.GetVolume().GetVolumeId(), tt.wantID)
				}
				if len(resp.GetVolume().GetAccessibleTopology()) == 0 {
					t.Error("the response carries no topology although the volume reports a zone")
				}
			}
		})
	}
}

func TestCreateSnapshotErrorFinality(t *testing.T) {
	sourceVolume := func(context.Context, string) (*cloud.Volume, error) {
		return &cloud.Volume{ID: "9000001", Name: "pvc-a", SizeBytes: 10 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-1",
			StatusCode: cloud.StatusAttached, OperationCode: cloud.OpNormal}, nil
	}
	cut := func() *cloud.Snapshot {
		return &cloud.Snapshot{ID: "9100001", Name: "snap-a", SourceVolumeID: "9000001", SizeBytes: 10 * utils.GiB,
			StatusCode: "INIT", OperationCode: "CREAT", CreatedAt: time.Now()}
	}
	notFound := func(context.Context, string) (*cloud.Snapshot, error) { return nil, cloud.ErrSnapshotNotFound }

	for _, tt := range []struct {
		name     string
		cloud    func() *cloud.MockCloud
		wantCode codes.Code
		wantID   string
	}{
		{
			name: "the name lookup fails",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetSnapshotByNameFn: func(context.Context, string) (*cloud.Snapshot, error) { return nil, transportTimeout },
				}
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "the source volume lookup fails",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetSnapshotByNameFn: notFound,
					GetVolumeByIDFn:     func(context.Context, string) (*cloud.Volume, error) { return nil, transportTimeout },
				}
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "the create times out on the wire",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetSnapshotByNameFn: notFound,
					GetVolumeByIDFn:     sourceVolume,
					CreateSnapshotFn: func(context.Context, *cloud.SnapshotOptions) (*cloud.Snapshot, error) {
						return nil, fmt.Errorf("failed to create snapshot: %w", transportTimeout)
					},
				}
			},
			wantCode: codes.Unavailable,
		},
		{
			name: "the API rejects the request",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetSnapshotByNameFn: notFound,
					GetVolumeByIDFn:     sourceVolume,
					CreateSnapshotFn:    func(context.Context, *cloud.SnapshotOptions) (*cloud.Snapshot, error) { return nil, apiRejection },
				}
			},
			wantCode: codes.Internal,
		},
		{
			name: "the name is in use: the snapshot behind it is returned",
			cloud: func() *cloud.MockCloud {
				var lookups int32
				return &cloud.MockCloud{
					GetSnapshotByNameFn: func(context.Context, string) (*cloud.Snapshot, error) {
						if atomic.AddInt32(&lookups, 1) == 1 {
							return nil, cloud.ErrSnapshotNotFound
						}
						return cut(), nil
					},
					GetVolumeByIDFn: sourceVolume,
					CreateSnapshotFn: func(context.Context, *cloud.SnapshotOptions) (*cloud.Snapshot, error) {
						return nil, fmt.Errorf("create snapshot: %w", cloud.ErrNameInUse)
					},
				}
			},
			wantCode: codes.OK,
			wantID:   "9100001",
		},
		{
			name: "cut, but the re-read fails: the create response stands in",
			cloud: func() *cloud.MockCloud {
				return &cloud.MockCloud{
					GetSnapshotByNameFn: notFound,
					GetVolumeByIDFn:     sourceVolume,
					CreateSnapshotFn:    func(context.Context, *cloud.SnapshotOptions) (*cloud.Snapshot, error) { return cut(), nil },
					GetSnapshotByIDFn:   func(context.Context, string) (*cloud.Snapshot, error) { return nil, transportTimeout },
				}
			},
			wantCode: codes.OK,
			wantID:   "9100001",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDriver(tt.cloud())
			resp, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "snap-a", SourceVolumeId: "9000001"})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if tt.wantID != "" && resp.GetSnapshot().GetSnapshotId() != tt.wantID {
				t.Errorf("snapshot ID = %q, want %q", resp.GetSnapshot().GetSnapshotId(), tt.wantID)
			}
		})
	}
}

// A volume restored from a snapshot looks the source up before the create.
// That read says nothing about whether an earlier attempt created the
// volume, so a failed read is Unavailable; a source that does not exist is
// NotFound, which no retry would change.
func TestCreateVolumeFromSnapshotSourceLookupFinality(t *testing.T) {
	for _, tt := range []struct {
		name     string
		lookup   func(context.Context, string) (*cloud.Snapshot, error)
		wantCode codes.Code
	}{
		{"lookup fails", func(context.Context, string) (*cloud.Snapshot, error) { return nil, transportTimeout }, codes.Unavailable},
		{"source missing", func(context.Context, string) (*cloud.Snapshot, error) { return nil, cloud.ErrSnapshotNotFound }, codes.NotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDriver(&cloud.MockCloud{
				GetVolumeByNameFn: func(context.Context, string) (*cloud.Volume, error) { return nil, cloud.ErrVolumeNotFound },
				GetSnapshotByIDFn: tt.lookup,
			})
			req := finalityCreateRequest("pvc-restored")
			req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "9100001"},
			}}
			_, err := d.CreateVolume(context.Background(), req)
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
		})
	}
}
