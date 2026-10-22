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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

func newTestDriver(mockCloud *cloud.MockCloud) *Driver {
	return &Driver{
		cloud:                 mockCloud,
		name:                  DriverName,
		publishInfoVolumeName: DriverName + "/volume-name",
		regionCode:            "KR",
		zoneCode:              "KR-1",
		serverInstanceNo:      "12345",
	}
}

func TestCreateVolume(t *testing.T) {
	tests := []struct {
		name       string
		req        *csi.CreateVolumeRequest
		setupCloud func(*cloud.MockCloud)
		wantCode   codes.Code
		wantVolID  string
	}{
		{
			name:     "missing volume name",
			req:      &csi.CreateVolumeRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing volume capabilities",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "idempotent - volume already exists",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, name string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "9000123", Name: "test-vol", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "9000123",
		},
		{
			name: "create new volume successfully",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.Name != "test-vol" {
						return nil, fmt.Errorf("unexpected name: %s", opts.Name)
					}
					if opts.SizeGiB != 20 {
						return nil, fmt.Errorf("unexpected size: %d", opts.SizeGiB)
					}
					// Volumes carry the same console marker as snapshots.
					if opts.Description != createdByCSI {
						return nil, fmt.Errorf("unexpected description: %q", opts.Description)
					}
					return &cloud.Volume{ID: "9000456", Name: opts.Name, SizeBytes: 20 * utils.GiB}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, id string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "9000456", Name: "test-vol", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "9000456",
		},
		{
			name: "unsupported volume type",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				Parameters: map[string]string{"type": "INVALID"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "FB1 below 100GiB minimum",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
				Parameters:    map[string]string{"type": "FB1"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			// A size outside the type's range is OUT_OF_RANGE, the code
			// the spec's CreateVolume table gives for an unsupported
			// capacity_range.
			wantCode: codes.OutOfRange,
		},
		{
			// HDD is XEN-generation vocabulary the original driver passed
			// to the API unchanged; it must fail fast instead of reaching
			// the backend.
			name: "XEN-generation HDD type is rejected",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				Parameters:    map[string]string{"type": "hdd"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.InvalidArgument,
		},
		{
			// A name collision with a XEN-generation volume (reported type
			// SSD/HDD) is never a retry match: the general type-mismatch
			// check must catch it.
			name: "name collision with XEN-generation volume returns AlreadyExists",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				Parameters:    map[string]string{"type": "CB1"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "9000077", Name: "test-vol", SizeBytes: 10 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "SSD",
					}, nil
				}
			},
			wantCode: codes.AlreadyExists,
		},
		{
			// SSD was the original nks-csi default StorageClass type, which
			// that driver itself rewrote to CB1 before calling the API; pin
			// the regression: this driver rejects it like any other type
			// outside the supported set.
			name: "XEN-generation SSD type is rejected",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				Parameters:    map[string]string{"type": "ssd"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "existing volume with mismatched type returns AlreadyExists",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				Parameters:    map[string]string{"type": "CB2"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-idem-type", Name: "test-vol", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode: codes.AlreadyExists,
		},
		{
			name: "existing volume outside requisite topology returns AlreadyExists",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				Parameters:    map[string]string{"type": "CB1"},
				AccessibilityRequirements: &csi.TopologyRequirement{
					Requisite: []*csi.Topology{
						{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}}, // zoneNo 3 = KR-2
					},
					Preferred: []*csi.Topology{
						{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-idem-zone", Name: "test-vol", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode: codes.AlreadyExists,
		},
		{
			name: "existing volume compatible via requisite despite preferred mismatch",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				Parameters:    map[string]string{"type": "CB1"},
				AccessibilityRequirements: &csi.TopologyRequirement{
					// Requisite allows both zones; Preferred[0] points at
					// KR-2 while the existing volume sits in KR-1. A
					// preference mismatch alone must not fail the retry.
					Requisite: []*csi.Topology{
						{Segments: map[string]string{"topology.kubernetes.io/zone": "2"}}, // zoneNo 2 = KR-1
						{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}}, // zoneNo 3 = KR-2
					},
					Preferred: []*csi.Topology{
						{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
						{Segments: map[string]string{"topology.kubernetes.io/zone": "2"}},
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-idem-zone-ok", Name: "test-vol", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "vol-idem-zone-ok",
		},
		{
			name: "existing volume exceeding capacity limit returns AlreadyExists",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB, LimitBytes: 20 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-idem-limit", Name: "test-vol", SizeBytes: 30 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode: codes.AlreadyExists,
		},
		{
			name: "existing volume within limit-only range is returned",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				// No RequiredBytes: any existing size up to the limit is
				// compatible even though the creation size would be 30GiB.
				CapacityRange: &csi.CapacityRange{LimitBytes: 30 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-idem-limit-only", Name: "test-vol", SizeBytes: 10 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "vol-idem-limit-only",
		},
		{
			// A limit-only range is a ceiling: a 25GiB limit must yield a
			// 20GiB volume (round down). Rounding up to 30GiB would exceed
			// the limit and wrongly reject a satisfiable request.
			name: "limit-only range rounds down to the allocation unit",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{LimitBytes: 25 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.SizeGiB != 20 {
						return nil, fmt.Errorf("expected 20 GiB for a 25GiB limit-only range, got %d", opts.SizeGiB)
					}
					return &cloud.Volume{ID: "vol-limit-down", Name: opts.Name, SizeBytes: 20 * utils.GiB}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-limit-down", Name: "test-vol", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "vol-limit-down",
		},
		{
			name: "limit-only range below the allocation unit is OutOfRange",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{LimitBytes: 5 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {},
			wantCode:   codes.OutOfRange,
		},
		{
			name: "requisite-only requirement places volume in first requisite zone",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				AccessibilityRequirements: &csi.TopologyRequirement{
					Requisite: []*csi.Topology{
						{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}}, // zoneNo 3 = KR-2
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.ZoneCode != "KR-2" {
						return nil, fmt.Errorf("expected requisite fallback zone KR-2, got %q", opts.ZoneCode)
					}
					return &cloud.Volume{ID: "vol-req-only", Name: opts.Name, SizeBytes: 10 * utils.GiB}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-req-only", Name: "test-vol", SizeBytes: 10 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-2", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "vol-req-only",
		},
		{
			name: "legacy zoneNo topology key is parsed",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				AccessibilityRequirements: &csi.TopologyRequirement{
					Preferred: []*csi.Topology{
						{Segments: map[string]string{"zoneNo": "3"}}, // legacy nks-csi key
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.ZoneCode != "KR-2" {
						return nil, fmt.Errorf("expected legacy zoneNo 3 to map to KR-2, got %q", opts.ZoneCode)
					}
					return &cloud.Volume{ID: "vol-legacy-key", Name: opts.Name, SizeBytes: 10 * utils.GiB}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-legacy-key", Name: "test-vol", SizeBytes: 10 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-2", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "vol-legacy-key",
		},
		{
			name: "topology requirement without supported zone key returns InvalidArgument",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 10 * utils.GiB},
				AccessibilityRequirements: &csi.TopologyRequirement{
					Requisite: []*csi.Topology{
						{Segments: map[string]string{"kubernetes.io/hostname": "node-1"}},
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "CB1 5GiB rounds up to 10GiB minimum",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 5 * utils.GiB},
				Parameters:    map[string]string{"type": "CB1"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.SizeGiB != 10 {
						return nil, fmt.Errorf("expected rounded-up size 10 GiB, got %d", opts.SizeGiB)
					}
					return &cloud.Volume{ID: "vol-cb1-min", Name: opts.Name, SizeBytes: 10 * utils.GiB}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-cb1-min", Name: "test-vol", SizeBytes: 10 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "vol-cb1-min",
		},
		{
			name: "size not aligned to 10GiB is rounded up",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 15 * utils.GiB},
				Parameters:    map[string]string{"type": "CB1"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.SizeGiB != 20 {
						return nil, fmt.Errorf("expected rounded-up size 20 GiB, got %d", opts.SizeGiB)
					}
					return &cloud.Volume{ID: "vol-round", Name: opts.Name, SizeBytes: 20 * utils.GiB}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "vol-round", Name: "test-vol", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "vol-round",
		},
		{
			name: "size rounded up exceeds limit",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				CapacityRange: &csi.CapacityRange{RequiredBytes: 15 * utils.GiB, LimitBytes: 15 * utils.GiB},
				Parameters:    map[string]string{"type": "CB1"},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.OutOfRange,
		},
		{
			name: "volume name exceeding 30 characters is shortened before reaching the cloud",
			req: &csi.CreateVolumeRequest{
				Name: "this-volume-name-is-way-too-long-for-ncloud",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				shortened := ncloudResourceName("this-volume-name-is-way-too-long-for-ncloud")
				mc.GetVolumeByNameFn = func(_ context.Context, name string) (*cloud.Volume, error) {
					if name != shortened {
						return nil, fmt.Errorf("lookup used name %q, want the shortened %q", name, shortened)
					}
					return nil, cloud.ErrVolumeNotFound
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.Name != shortened {
						return nil, fmt.Errorf("create used name %q, want the shortened %q", opts.Name, shortened)
					}
					return &cloud.Volume{ID: "vol-short", Name: opts.Name, SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1"}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "vol-short", Name: shortened, SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1", ZoneCode: "KR-1"}, nil
				}
			},
			wantVolID: "vol-short",
		},
		{
			name: "create volume with snapshot source",
			req: &csi.CreateVolumeRequest{
				Name: "test-vol-from-snap",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
						AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					},
				},
				VolumeContentSource: &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-100"},
					},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByNameFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
				mc.GetSnapshotByIDFn = func(_ context.Context, id string) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{ID: "snap-100", SourceVolumeID: "9000010"}, nil
				}
				mc.CreateVolumeFn = func(_ context.Context, opts *cloud.VolumeOptions) (*cloud.Volume, error) {
					if opts.SnapshotID != "snap-100" {
						return nil, fmt.Errorf("expected snapshotID snap-100, got %s", opts.SnapshotID)
					}
					return &cloud.Volume{ID: "9000789", Name: opts.Name}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{
						ID: "9000789", Name: "test-vol-from-snap", SizeBytes: 20 * utils.GiB,
						RegionCode: "KR", ZoneCode: "KR-1", VolumeTypeCode: "CB1",
					}, nil
				}
			},
			wantCode:  codes.OK,
			wantVolID: "9000789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &cloud.MockCloud{}
			if tt.setupCloud != nil {
				tt.setupCloud(mc)
			}
			d := newTestDriver(mc)

			resp, err := d.CreateVolume(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tt.wantCode)
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Fatalf("expected gRPC status error, got: %v", err)
				}
				if st.Code() != tt.wantCode {
					t.Errorf("expected code %v, got %v: %s", tt.wantCode, st.Code(), st.Message())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Volume.VolumeId != tt.wantVolID {
				t.Errorf("expected volumeId %s, got %s", tt.wantVolID, resp.Volume.VolumeId)
			}
		})
	}
}

func TestDeleteVolume(t *testing.T) {
	tests := []struct {
		name       string
		req        *csi.DeleteVolumeRequest
		setupCloud func(*cloud.MockCloud)
		wantCode   codes.Code
	}{
		{
			name:     "missing volume ID",
			req:      &csi.DeleteVolumeRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "volume not found - idempotent",
			req:  &csi.DeleteVolumeRequest{VolumeId: "9404002"},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.OK,
		},
		{
			name: "delete volume successfully",
			req:  &csi.DeleteVolumeRequest{VolumeId: "9000123"},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000123", StatusCode: "ATTAC"}, nil
				}
				mc.EnsureDetachVolumeFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000123", StatusCode: "CREAT"}, nil
				}
				mc.WaitForVolumeStatusFn = func(_ context.Context, _ string, _ []string) error {
					return nil
				}
				mc.DeleteVolumeFn = func(_ context.Context, _ string) error {
					return nil
				}
			},
			wantCode: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &cloud.MockCloud{}
			if tt.setupCloud != nil {
				tt.setupCloud(mc)
			}
			d := newTestDriver(mc)

			_, err := d.DeleteVolume(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tt.wantCode)
				}
				st, _ := status.FromError(err)
				if st.Code() != tt.wantCode {
					t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestControllerPublishVolume(t *testing.T) {
	tests := []struct {
		name       string
		req        *csi.ControllerPublishVolumeRequest
		setupCloud func(*cloud.MockCloud)
		wantCode   codes.Code
	}{
		{
			name:     "missing volume ID",
			req:      &csi.ControllerPublishVolumeRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing node ID",
			req:      &csi.ControllerPublishVolumeRequest{VolumeId: "9000001"},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "already attached to correct node",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId: "9000001",
				NodeId:   "node-1",
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test", ServerInstanceNo: "node-1", StatusCode: "ATTAC", OperationCode: "NULL"}, nil
				}
			},
			wantCode: codes.OK,
		},
		{
			// An operation that leaves the attachment alone (snapshot,
			// resize) must not turn a satisfied request into a retry.
			name: "same node with a non-detach operation still succeeds",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId: "9000001",
				NodeId:   "node-1",
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test", ServerInstanceNo: "node-1", StatusCode: "ATTAC", OperationCode: "CHNG"}, nil
				}
			},
			wantCode: codes.OK,
		},
		{
			name: "same node but detach in progress returns Aborted",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId: "9000001",
				NodeId:   "node-1",
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test", ServerInstanceNo: "node-1", StatusCode: "ATTAC", OperationCode: "DETAC"}, nil
				}
			},
			wantCode: codes.Aborted,
		},
		{
			name: "attach to new node successfully",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId: "9000001",
				NodeId:   "node-2",
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				callCount := 0
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					callCount++
					if callCount <= 1 {
						return &cloud.Volume{ID: "9000001", Name: "test", ServerInstanceNo: "", StatusCode: "CREAT"}, nil
					}
					return &cloud.Volume{ID: "9000001", Name: "test", ServerInstanceNo: "node-2", StatusCode: "ATTAC"}, nil
				}
				mc.AttachVolumeFn = func(_ context.Context, _, _ string) error {
					return nil
				}
				mc.WaitForVolumeStatusFn = func(_ context.Context, _ string, _ []string) error {
					return nil
				}
			},
			wantCode: codes.OK,
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

			if tt.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tt.wantCode)
				}
				st, _ := status.FromError(err)
				if st.Code() != tt.wantCode {
					t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestControllerExpandVolume(t *testing.T) {
	tests := []struct {
		name       string
		req        *csi.ControllerExpandVolumeRequest
		setupCloud func(*cloud.MockCloud)
		wantCode   codes.Code
		wantSize   int64
	}{
		{
			name:     "missing volume ID",
			req:      &csi.ControllerExpandVolumeRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "expand volume successfully",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "NULL"}, nil
				}
				mc.ResizeVolumeFn = func(_ context.Context, id string, sizeGiB int32) error {
					if sizeGiB != 40 {
						return fmt.Errorf("expected 40 GiB, got %d", sizeGiB)
					}
					return nil
				}
				mc.WaitForVolumeResizeFn = func(_ context.Context, id string, minSizeBytes int64) error {
					if minSizeBytes != 40*utils.GiB {
						return fmt.Errorf("expected wait for %d bytes, got %d", 40*utils.GiB, minSizeBytes)
					}
					return nil
				}
			},
			wantCode: codes.OK,
			wantSize: 40 * utils.GiB,
		},
		{
			// Limit-only expand: 25GiB rounds down to 20GiB, which the
			// volume already satisfies — succeed without a backend resize.
			name: "limit-only expand within the limit is a no-op",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{LimitBytes: 25 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "NULL"}, nil
				}
			},
			wantCode: codes.OK,
			wantSize: 20 * utils.GiB,
		},
		{
			name: "expand is Aborted while another operation is in progress",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "CHNG"}, nil
				}
			},
			wantCode: codes.Aborted,
		},
		{
			// The backend resize API is asynchronous; success must not be
			// reported until the volume actually converges.
			name: "expand fails when the backend resize does not converge",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "NULL"}, nil
				}
				mc.ResizeVolumeFn = func(_ context.Context, _ string, _ int32) error { return nil }
				mc.WaitForVolumeResizeFn = func(_ context.Context, _ string, _ int64) error {
					return fmt.Errorf("timeout waiting for volume 9000001 to resize")
				}
			},
			wantCode: codes.Aborted,
		},
		{
			name: "missing volume returns NotFound",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9404001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.NotFound,
		},
		{
			name: "no-op when volume already at requested size",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 40 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "NULL"}, nil
				}
				mc.ResizeVolumeFn = func(_ context.Context, _ string, _ int32) error {
					return fmt.Errorf("resize must not be called for a satisfied request")
				}
				mc.WaitForVolumeResizeFn = func(_ context.Context, _ string, _ int64) error {
					return fmt.Errorf("wait must not be called when no operation is in progress")
				}
			},
			wantCode: codes.OK,
			wantSize: 40 * utils.GiB,
		},
		{
			// A retried expand can find the size already reported while the
			// backend operation is still running; success must wait for it
			// to settle instead of trusting the size field alone.
			name: "satisfied size with in-flight operation waits for settle",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 40 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "CHNG"}, nil
				}
				mc.ResizeVolumeFn = func(_ context.Context, _ string, _ int32) error {
					return fmt.Errorf("resize must not be called for a satisfied request")
				}
				mc.WaitForVolumeResizeFn = func(_ context.Context, _ string, minSizeBytes int64) error {
					if minSizeBytes != 40*utils.GiB {
						return fmt.Errorf("expected wait for %d bytes, got %d", 40*utils.GiB, minSizeBytes)
					}
					return nil
				}
			},
			wantCode: codes.OK,
			wantSize: 40 * utils.GiB,
		},
		{
			name: "satisfied size with in-flight operation that never settles",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 40 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "CHNG"}, nil
				}
				mc.WaitForVolumeResizeFn = func(_ context.Context, _ string, _ int64) error {
					return fmt.Errorf("timeout waiting for volume 9000001 to resize")
				}
			},
			wantCode: codes.Aborted,
		},
		{
			name: "XEN-generation volume returns FailedPrecondition",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 40 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "xen-vol", SizeBytes: 20 * utils.GiB, VolumeTypeCode: "HDD", OperationCode: "NULL"}, nil
				}
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "shrink request returns current size without resizing",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 40 * utils.GiB, VolumeTypeCode: "CB1", OperationCode: "NULL"}, nil
				}
				mc.ResizeVolumeFn = func(_ context.Context, _ string, _ int32) error {
					return fmt.Errorf("resize must not be called for a shrink request")
				}
			},
			wantCode: codes.OK,
			wantSize: 40 * utils.GiB,
		},
		{
			name: "current size exceeding requested limit returns OutOfRange",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 50 * utils.GiB, LimitBytes: 80 * utils.GiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 100 * utils.GiB, VolumeTypeCode: "CB1"}, nil
				}
				mc.ResizeVolumeFn = func(_ context.Context, _ string, _ int32) error {
					return fmt.Errorf("resize must not be called when the limit cannot be satisfied")
				}
			},
			wantCode: codes.OutOfRange,
		},
		{
			name: "expansion beyond volume type maximum returns OutOfRange",
			req: &csi.ControllerExpandVolumeRequest{
				VolumeId:      "9000001",
				CapacityRange: &csi.CapacityRange{RequiredBytes: 17 * utils.TiB},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: "9000001", Name: "test-vol", SizeBytes: 20 * utils.GiB, VolumeTypeCode: "CB1"}, nil
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

			resp, err := d.ControllerExpandVolume(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tt.wantCode)
				}
				st, _ := status.FromError(err)
				if st.Code() != tt.wantCode {
					t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.CapacityBytes != tt.wantSize {
				t.Errorf("expected capacity %d, got %d", tt.wantSize, resp.CapacityBytes)
			}
		})
	}
}

func TestCreateSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		req        *csi.CreateSnapshotRequest
		setupCloud func(*cloud.MockCloud)
		wantCode   codes.Code
		wantSnapID string
	}{
		{
			name:     "missing snapshot name",
			req:      &csi.CreateSnapshotRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "create snapshot successfully",
			req: &csi.CreateSnapshotRequest{
				Name:           "snap-1",
				SourceVolumeId: "9000001",
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, id string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: id, Name: "test-vol", SizeBytes: 20 * utils.GiB}, nil
				}
				mc.GetSnapshotByNameFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return nil, cloud.ErrSnapshotNotFound
				}
				mc.CreateSnapshotFn = func(_ context.Context, opts *cloud.SnapshotOptions) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{
						ID: "snap-100", Name: opts.Name, SourceVolumeID: opts.SourceVolumeID,
						CreatedAt: time.Now(), ReadyToUse: false,
					}, nil
				}
				mc.GetSnapshotByIDFn = func(_ context.Context, id string) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{
						ID: "snap-100", Name: "snap-1", SourceVolumeID: "9000001",
						SizeBytes: 20 * utils.GiB, CreatedAt: time.Now(), ReadyToUse: true,
					}, nil
				}
			},
			wantCode:   codes.OK,
			wantSnapID: "snap-100",
		},
		{
			name: "missing source volume without existing snapshot returns NotFound",
			req: &csi.CreateSnapshotRequest{
				Name:           "snap-1",
				SourceVolumeId: "9404001",
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetSnapshotByNameFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return nil, cloud.ErrSnapshotNotFound
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.NotFound,
		},
		{
			// Idempotent retry: the snapshot was cut successfully and its
			// source volume was deleted afterwards — the retry must still
			// return the existing snapshot, not NotFound.
			name: "existing snapshot is returned even when the source volume is gone",
			req: &csi.CreateSnapshotRequest{
				Name:           "snap-1",
				SourceVolumeId: "9000001",
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetSnapshotByNameFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{
						ID: "snap-100", Name: "snap-1", SourceVolumeID: "9000001",
						SizeBytes: 20 * utils.GiB, CreatedAt: time.Now(), ReadyToUse: true,
					}, nil
				}
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, fmt.Errorf("source volume lookup must not run for an idempotent retry")
				}
			},
			wantCode:   codes.OK,
			wantSnapID: "snap-100",
		},
		{
			name: "existing snapshot with different source wins over missing requested source",
			req: &csi.CreateSnapshotRequest{
				Name:           "snap-1",
				SourceVolumeId: "9404001",
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetSnapshotByNameFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{
						ID: "snap-100", Name: "snap-1", SourceVolumeID: "9000012",
						CreatedAt: time.Now(), ReadyToUse: true,
					}, nil
				}
			},
			wantCode: codes.AlreadyExists,
		},
		{
			name: "existing snapshot with different source volume returns AlreadyExists",
			req: &csi.CreateSnapshotRequest{
				Name:           "snap-1",
				SourceVolumeId: "9000001",
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, id string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: id, Name: "test-vol", SizeBytes: 20 * utils.GiB}, nil
				}
				mc.GetSnapshotByNameFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{
						ID: "snap-100", Name: "snap-1", SourceVolumeID: "9000012",
						CreatedAt: time.Now(), ReadyToUse: true,
					}, nil
				}
			},
			wantCode: codes.AlreadyExists,
		},
		{
			name: "existing snapshot with same source volume is returned",
			req: &csi.CreateSnapshotRequest{
				Name:           "snap-1",
				SourceVolumeId: "9000001",
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, id string) (*cloud.Volume, error) {
					return &cloud.Volume{ID: id, Name: "test-vol", SizeBytes: 20 * utils.GiB}, nil
				}
				mc.GetSnapshotByNameFn = func(_ context.Context, _ string) (*cloud.Snapshot, error) {
					return &cloud.Snapshot{
						ID: "snap-100", Name: "snap-1", SourceVolumeID: "9000001",
						SizeBytes: 20 * utils.GiB, CreatedAt: time.Now(), ReadyToUse: true,
					}, nil
				}
			},
			wantCode:   codes.OK,
			wantSnapID: "snap-100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &cloud.MockCloud{}
			if tt.setupCloud != nil {
				tt.setupCloud(mc)
			}
			d := newTestDriver(mc)

			resp, err := d.CreateSnapshot(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tt.wantCode)
				}
				st, _ := status.FromError(err)
				if st.Code() != tt.wantCode {
					t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Snapshot.SnapshotId != tt.wantSnapID {
				t.Errorf("expected snapshotId %s, got %s", tt.wantSnapID, resp.Snapshot.SnapshotId)
			}
		})
	}
}

func TestValidateVolumeCapabilities(t *testing.T) {
	supportedCap := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	}
	unsupportedCap := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	}
	volumeExists := func(mc *cloud.MockCloud) {
		mc.GetVolumeByIDFn = func(_ context.Context, id string) (*cloud.Volume, error) {
			return &cloud.Volume{ID: id, Name: "test-vol", SizeBytes: 10 * utils.GiB}, nil
		}
	}

	tests := []struct {
		name          string
		req           *csi.ValidateVolumeCapabilitiesRequest
		setupCloud    func(*cloud.MockCloud)
		wantCode      codes.Code
		wantConfirmed bool
	}{
		{
			name: "supported capabilities are confirmed as requested",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId:           "9000001",
				VolumeCapabilities: []*csi.VolumeCapability{supportedCap},
			},
			setupCloud:    volumeExists,
			wantCode:      codes.OK,
			wantConfirmed: true,
		},
		{
			name: "unsupported access mode is not confirmed",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId:           "9000001",
				VolumeCapabilities: []*csi.VolumeCapability{unsupportedCap},
			},
			setupCloud:    volumeExists,
			wantCode:      codes.OK,
			wantConfirmed: false,
		},
		{
			name: "missing volume returns NotFound",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId:           "9404001",
				VolumeCapabilities: []*csi.VolumeCapability{supportedCap},
			},
			setupCloud: func(mc *cloud.MockCloud) {
				mc.GetVolumeByIDFn = func(_ context.Context, _ string) (*cloud.Volume, error) {
					return nil, cloud.ErrVolumeNotFound
				}
			},
			wantCode: codes.NotFound,
		},
		{
			name: "nil capabilities returns InvalidArgument",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: "9000001",
			},
			setupCloud: volumeExists,
			wantCode:   codes.InvalidArgument,
		},
		{
			name: "empty capabilities slice returns InvalidArgument",
			req: &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId:           "9000001",
				VolumeCapabilities: []*csi.VolumeCapability{},
			},
			setupCloud: volumeExists,
			wantCode:   codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &cloud.MockCloud{}
			if tt.setupCloud != nil {
				tt.setupCloud(mc)
			}
			d := newTestDriver(mc)

			resp, err := d.ValidateVolumeCapabilities(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				if err == nil {
					t.Fatalf("expected error with code %v, got nil", tt.wantCode)
				}
				st, _ := status.FromError(err)
				if st.Code() != tt.wantCode {
					t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantConfirmed {
				if resp.Confirmed == nil {
					t.Fatalf("expected capabilities to be confirmed, got message %q", resp.Message)
				}
				if len(resp.Confirmed.VolumeCapabilities) != len(tt.req.VolumeCapabilities) {
					t.Errorf("expected %d confirmed capabilities, got %d",
						len(tt.req.VolumeCapabilities), len(resp.Confirmed.VolumeCapabilities))
				}
			} else {
				if resp.Confirmed != nil {
					t.Errorf("expected capabilities NOT to be confirmed")
				}
				if resp.Message == "" {
					t.Errorf("expected a message explaining the rejected capabilities")
				}
			}
		})
	}
}
