// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/mount"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

func TestNodeGetInfoAdvertisesBothTopologyKeys(t *testing.T) {
	d := &Driver{
		serverInstanceNo:  "12345",
		zoneCode:          "KR-1",
		topologyZoneValue: "2",
		maxVolumesPerNode: 15,
	}

	resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.MaxVolumesPerNode != 15 {
		t.Errorf("expected maxVolumesPerNode 15, got %d", resp.MaxVolumesPerNode)
	}

	segments := resp.GetAccessibleTopology().GetSegments()
	if got := segments["topology.kubernetes.io/zone"]; got != "2" {
		t.Errorf("expected standard zone segment %q, got %q", "2", got)
	}
	// The legacy nks-csi key must be advertised alongside the standard key
	// so pre-migration PVs and StorageClasses keep scheduling.
	if got := segments[legacyTopologyZoneKey]; got != "2" {
		t.Errorf("expected legacy zoneNo segment %q, got %q", "2", got)
	}
}

func TestVerifyMountedDevice(t *testing.T) {
	tests := []struct {
		name           string
		mountedDevice  string
		expectedDevice string
		wantCode       codes.Code
	}{
		{name: "matching device", mountedDevice: "/dev/vdb", expectedDevice: "/dev/vdb", wantCode: codes.OK},
		{name: "bind subpath suffix is normalized", mountedDevice: "/dev/vdb[/]", expectedDevice: "/dev/vdb", wantCode: codes.OK},
		{name: "foreign device is rejected", mountedDevice: "/dev/vdc", expectedDevice: "/dev/vdb", wantCode: codes.AlreadyExists},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Driver{mounter: &mount.MockMounter{
				GetDevicePathFromMountFn: func(_ string) (string, error) {
					return tt.mountedDevice, nil
				},
			}}

			err := d.verifyMountedDevice("/target", tt.expectedDevice)
			if tt.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v", tt.wantCode, err)
			}
		})
	}
}

func TestVerifyMountReadOnly(t *testing.T) {
	tests := []struct {
		name         string
		options      []string
		wantReadOnly bool
		wantCode     codes.Code
	}{
		{name: "rw mount matches rw request", options: []string{"rw", "relatime"}, wantReadOnly: false, wantCode: codes.OK},
		{name: "ro mount matches ro request", options: []string{"ro", "relatime"}, wantReadOnly: true, wantCode: codes.OK},
		{name: "ro mount rejects rw request", options: []string{"ro", "relatime"}, wantReadOnly: false, wantCode: codes.AlreadyExists},
		{name: "rw mount rejects ro request", options: []string{"rw", "relatime"}, wantReadOnly: true, wantCode: codes.AlreadyExists},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Driver{mounter: &mount.MockMounter{
				GetMountOptionsFn: func(_ string) ([]string, error) {
					return tt.options, nil
				},
			}}

			err := d.verifyMountReadOnly("/target", tt.wantReadOnly)
			if tt.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v", tt.wantCode, err)
			}
		})
	}
}

func TestNodePublishVolumeReadOnlyCompatibility(t *testing.T) {
	newRequest := func(readonly bool, mountFlags []string) *csi.NodePublishVolumeRequest {
		return &csi.NodePublishVolumeRequest{
			VolumeId:          "1234567",
			StagingTargetPath: "/staging",
			TargetPath:        "/target",
			Readonly:          readonly,
			VolumeCapability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{MountFlags: mountFlags}},
			},
		}
	}

	tests := []struct {
		name         string
		req          *csi.NodePublishVolumeRequest
		mountOptions []string
		wantCode     codes.Code
	}{
		{
			name:         "rw retry over rw mount succeeds",
			req:          newRequest(false, nil),
			mountOptions: []string{"rw", "relatime"},
			wantCode:     codes.OK,
		},
		{
			name:         "ro flag from StorageClass mountOptions matches ro mount",
			req:          newRequest(false, []string{"ro"}),
			mountOptions: []string{"ro", "relatime"},
			wantCode:     codes.OK,
		},
		{
			name:         "readonly request over rw mount is rejected",
			req:          newRequest(true, nil),
			mountOptions: []string{"rw", "relatime"},
			wantCode:     codes.AlreadyExists,
		},
		{
			name:         "rw request over ro mount is rejected",
			req:          newRequest(false, nil),
			mountOptions: []string{"ro", "relatime"},
			wantCode:     codes.AlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Driver{mounter: &mount.MockMounter{
				IsMountedFn: func(_ string) (bool, error) { return true, nil },
				GetDevicePathFromMountFn: func(_ string) (string, error) {
					return "/dev/vdb", nil
				},
				GetMountOptionsFn: func(_ string) ([]string, error) {
					return tt.mountOptions, nil
				},
			}}

			_, err := d.NodePublishVolume(context.Background(), tt.req)
			if tt.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error with code %v, got nil", tt.wantCode)
			}
			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v", tt.wantCode, err)
			}
		})
	}
}

func stubDeviceResolution(t *testing.T, device string) {
	t.Helper()
	orig := resolveDevicePath
	resolveDevicePath = func(_ context.Context, _ deviceDiscoveryOptions, _, _ string) (string, error) { return device, nil }
	t.Cleanup(func() { resolveDevicePath = orig })
}

func TestNodeStageVolumeIdempotentRetry(t *testing.T) {
	newRequest := func(fsType string, volumeContext map[string]string) *csi.NodeStageVolumeRequest {
		return &csi.NodeStageVolumeRequest{
			VolumeId:          "1234567",
			StagingTargetPath: "/staging",
			VolumeContext:     volumeContext,
			VolumeCapability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: fsType}},
			},
		}
	}

	tests := []struct {
		name          string
		req           *csi.NodeStageVolumeRequest
		mountedDevice string
		actualFsType  string
		wantCode      codes.Code
	}{
		{
			name:          "retry over compatible mount succeeds without formatting",
			req:           newRequest("ext4", nil),
			mountedDevice: "/dev/vdb",
			actualFsType:  "ext4",
			wantCode:      codes.OK,
		},
		{
			name:          "foreign device on target is rejected",
			req:           newRequest("ext4", nil),
			mountedDevice: "/dev/vdz",
			actualFsType:  "ext4",
			wantCode:      codes.AlreadyExists,
		},
		{
			name:          "filesystem type mismatch is rejected",
			req:           newRequest("xfs", nil),
			mountedDevice: "/dev/vdb",
			actualFsType:  "ext4",
			wantCode:      codes.AlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubDeviceResolution(t, "/dev/vdb")
			d := &Driver{mounter: &mount.MockMounter{
				IsMountedFn: func(_ string) (bool, error) { return true, nil },
				GetDevicePathFromMountFn: func(_ string) (string, error) {
					return tt.mountedDevice, nil
				},
				GetFsTypeFn: func(_ string) (string, error) { return tt.actualFsType, nil },
				FormatFn: func(_, _ string) error {
					return fmt.Errorf("format must not be called for an already-mounted staging target")
				},
				MountFn: func(_, _, _ string, _ ...string) error {
					return fmt.Errorf("mount must not be called for an already-mounted staging target")
				},
			}}

			_, err := d.NodeStageVolume(context.Background(), tt.req)
			if tt.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
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

func TestNodePublishVolumeBlockDeviceIdentity(t *testing.T) {
	newRequest := func() *csi.NodePublishVolumeRequest {
		return &csi.NodePublishVolumeRequest{
			VolumeId:          "1234567",
			StagingTargetPath: "/staging",
			TargetPath:        "/target",
			VolumeCapability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			},
		}
	}

	tests := []struct {
		name       string
		sameDevice bool
		wantCode   codes.Code
	}{
		{name: "same device retry succeeds", sameDevice: true, wantCode: codes.OK},
		{name: "different device on target is rejected", sameDevice: false, wantCode: codes.AlreadyExists},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubDeviceResolution(t, "/dev/vdb")
			d := &Driver{mounter: &mount.MockMounter{
				IsMountedFn:       func(_ string) (bool, error) { return true, nil },
				SameDeviceFn:      func(_, _ string) (bool, error) { return tt.sameDevice, nil },
				GetMountOptionsFn: func(_ string) ([]string, error) { return []string{"rw"}, nil },
			}}

			_, err := d.NodePublishVolume(context.Background(), newRequest())
			if tt.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
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

func TestNodeExpandVolumeValidation(t *testing.T) {
	tests := []struct {
		name     string
		req      *csi.NodeExpandVolumeRequest
		wantCode codes.Code
	}{
		{
			name:     "missing volume ID",
			req:      &csi.NodeExpandVolumeRequest{VolumePath: "/mnt/vol", CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB}},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing volume path",
			req:      &csi.NodeExpandVolumeRequest{VolumeId: "1234567", CapacityRange: &csi.CapacityRange{RequiredBytes: 20 * utils.GiB}},
			wantCode: codes.InvalidArgument,
		},
		{
			// capacity_range is optional: the request proceeds to the path
			// checks and fails there because nothing is mounted.
			name:     "missing capacity range",
			req:      &csi.NodeExpandVolumeRequest{VolumeId: "1234567", VolumePath: "/mnt/vol"},
			wantCode: codes.NotFound,
		},
		{
			name: "empty capacity range",
			req: &csi.NodeExpandVolumeRequest{
				VolumeId: "1234567", VolumePath: "/mnt/vol",
				CapacityRange: &csi.CapacityRange{},
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Driver{mounter: &mount.MockMounter{
				IsMountedFn: func(string) (bool, error) { return false, nil },
			}}

			_, err := d.NodeExpandVolume(context.Background(), tt.req)
			if err == nil {
				t.Fatalf("expected error with code %v, got nil", tt.wantCode)
			}
			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v", tt.wantCode, err)
			}
		})
	}
}

func TestMaxVolumesOverrideFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int64
		wantErr bool
	}{
		{name: "unset returns zero", value: "", want: 0},
		{name: "positive integer", value: "25", want: 25},
		{name: "zero is invalid", value: "0", wantErr: true},
		{name: "negative is invalid", value: "-3", wantErr: true},
		{name: "non-numeric is invalid", value: "many", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxVolumesPerNodeEnv, tt.value)
			got, err := maxVolumesOverrideFromEnv()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

// Regression tests for the /dev/disk/by-id escape: a crafted volume ID or
// volumeHandle must never resolve to an arbitrary device node (e.g. the
// node's root disk) and must be rejected before any path construction.
func TestFindAbsoluteDeviceByIDPathRejectsEscape(t *testing.T) {
	for _, name := range []string{"/../../../vda", "../null", "1234/../../sda", "/dev/null"} {
		if _, err := findAbsoluteDeviceByIDPath(context.Background(), deviceDiscoveryOptions{byIDDir: diskIDPath}, diskNcloudKVMPrefix, name); err == nil {
			t.Errorf("expected error for volume name %q, got nil", name)
		}
	}
}

func TestGetVolumeSourceRejectsInvalidVolumeID(t *testing.T) {
	d := &Driver{serverInstanceNo: "200000002"}
	for _, id := range []string{"", "vol-1", "/../../../vda", "123 456", "12a34", "-1234"} {
		if _, err := d.getVolumeSource(context.Background(), id, nil, false); err == nil {
			t.Errorf("expected error for volume ID %q, got nil", id)
		}
	}
}

// PVs provisioned by the original nks-csi AND the regionNo and zoneNo
// labels in their nodeAffinity: when the operator supplies the legacy
// region number, new nodes must advertise it or those PVs stop scheduling.
func TestNodeGetInfoAdvertisesLegacyRegionNo(t *testing.T) {
	d := &Driver{
		serverInstanceNo:  "12345",
		zoneCode:          "KR-1",
		topologyZoneValue: "2",
		legacyRegionNo:    "1",
	}

	resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := resp.GetAccessibleTopology().GetSegments()[legacyTopologyRegionKey]; got != "1" {
		t.Errorf("expected legacy regionNo segment %q, got %q", "1", got)
	}
}

func TestNodeGetInfoOmitsLegacyRegionNoWhenUnset(t *testing.T) {
	d := &Driver{
		serverInstanceNo:  "12345",
		zoneCode:          "KR-1",
		topologyZoneValue: "2",
	}

	resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := resp.GetAccessibleTopology().GetSegments()[legacyTopologyRegionKey]; ok {
		t.Error("legacy regionNo segment must not be advertised when the option is unset")
	}
}

func TestNewDriverRejectsInvalidLegacyRegionNo(t *testing.T) {
	for _, v := range []string{"KR", "-1", "1a", "1 "} {
		_, err := NewDriver(&cloud.MockCloud{}, nil, &Options{
			Endpoint:       "unix:///tmp/csi-test.sock",
			Mode:           ControllerMode,
			LegacyRegionNo: v,
		})
		if err == nil {
			t.Errorf("expected error for legacy region no %q, got nil", v)
		}
	}
}

func TestNodeGetVolumeStatsAssociation(t *testing.T) {
	const volID = "1234567"
	device := "/dev/sanity-" + volID

	newStatsDriver := func() (*Driver, *mount.FakeMounter) {
		fm := mount.NewFakeMounter()
		return &Driver{mounter: fm, defaultFsType: "ext4"}, fm
	}
	statsReq := func(path string) *csi.NodeGetVolumeStatsRequest {
		return &csi.NodeGetVolumeStatsRequest{VolumeId: volID, VolumePath: path}
	}
	expectCode := func(t *testing.T, err error, want codes.Code) {
		t.Helper()
		if err == nil {
			t.Fatalf("expected error with code %v, got nil", want)
		}
		if st, _ := status.FromError(err); st.Code() != want {
			t.Errorf("expected code %v, got %v", want, err)
		}
	}

	t.Run("unmounted path returns NotFound", func(t *testing.T) {
		d, _ := newStatsDriver()
		stubDeviceResolution(t, device)

		_, err := d.NodeGetVolumeStats(context.Background(), statsReq("/not/mounted"))
		expectCode(t, err, codes.NotFound)
	})

	t.Run("volume unknown on this node returns NotFound", func(t *testing.T) {
		d, fm := newStatsDriver()
		orig := resolveDevicePath
		resolveDevicePath = func(_ context.Context, _ deviceDiscoveryOptions, _, _ string) (string, error) {
			return "", fmt.Errorf("no by-id entry")
		}
		t.Cleanup(func() { resolveDevicePath = orig })
		if err := fm.Mount(device, "/mnt/vol", "ext4"); err != nil {
			t.Fatal(err)
		}

		_, err := d.NodeGetVolumeStats(context.Background(), statsReq("/mnt/vol"))
		expectCode(t, err, codes.NotFound)
	})

	t.Run("path backed by a different device returns NotFound", func(t *testing.T) {
		d, fm := newStatsDriver()
		stubDeviceResolution(t, device)
		if err := fm.Mount("/dev/other-device", "/mnt/vol", "ext4"); err != nil {
			t.Fatal(err)
		}

		_, err := d.NodeGetVolumeStats(context.Background(), statsReq("/mnt/vol"))
		expectCode(t, err, codes.NotFound)
	})

	t.Run("matching mount returns statistics", func(t *testing.T) {
		d, fm := newStatsDriver()
		stubDeviceResolution(t, device)
		if err := fm.Mount(device, "/mnt/vol", "ext4"); err != nil {
			t.Fatal(err)
		}

		resp, err := d.NodeGetVolumeStats(context.Background(), statsReq("/mnt/vol"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resp.GetUsage()) != 2 {
			t.Errorf("expected bytes+inodes usage entries, got %d", len(resp.GetUsage()))
		}
	})
}

// A node in a zone the driver cannot number has no value to put in
// topology.kubernetes.io/zone: the kubelet would reject the plugin with a
// topology value collision, so startup fails with the supported zones and
// the reason instead.
func TestNewDriverRefusesUnsupportedNodeZone(t *testing.T) {
	startFakeIMDSInZone(t, "PCS-1")

	_, err := NewDriver(&cloud.MockCloud{}, &mount.MockMounter{}, &Options{
		Endpoint: "unix:///tmp/csi-test.sock", Mode: NodeMode, DefaultFsType: "ext4", RegionCode: "KR",
	})
	if err == nil {
		t.Fatal("expected startup to fail on an unsupported zone, got nil")
	}
	for _, want := range []string{"PCS-1", "KR-1=2", "driver release"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("startup error does not mention %q: %v", want, err)
		}
	}
}

// A block volume attaches only within its own zone, so a regionCode that
// is not the node's region sends every API call to a region holding none
// of this node's volumes. The failure is silent at startup and shows up
// later as a volume that was just created not being found, so startup
// refuses instead.
func TestNewDriverRefusesRegionCodeThatIsNotTheNodeRegion(t *testing.T) {
	startFakeIMDSInZone(t, "KR-2")

	_, err := NewDriver(&cloud.MockCloud{}, &mount.MockMounter{}, &Options{
		Endpoint: "unix:///tmp/csi-test.sock", Mode: NodeMode, DefaultFsType: "ext4", RegionCode: "SGN",
	})
	if err == nil {
		t.Fatal("expected startup to fail on a regionCode outside the node's region, got nil")
	}
	// Both values belong in the message: neither alone tells an operator
	// which of the two to change.
	for _, want := range []string{"regionCode", "SGN", "KR-2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("startup error does not mention %q: %v", want, err)
		}
	}
}

// The region is compared case-insensitively, the way the API accepts it.
func TestNewDriverAcceptsRegionCodeCasing(t *testing.T) {
	startFakeIMDSInZone(t, "KR-2")

	mc := &cloud.MockCloud{
		GetServerInstanceFn: func(_ context.Context, _ string) (*cloud.ServerInstance, error) {
			return &cloud.ServerInstance{MaxBlockStorageCount: 20}, nil
		},
	}
	if _, err := NewDriver(mc, &mount.MockMounter{}, &Options{
		Endpoint: "unix:///tmp/csi-test.sock", Mode: NodeMode, DefaultFsType: "ext4", RegionCode: "kr",
	}); err != nil {
		t.Errorf("lowercase regionCode should be accepted: %v", err)
	}
}

// Bind-mounting an unmounted staging directory would hand the pod the
// node's root filesystem; NodePublishVolume must refuse until the staged
// mount exists.
func TestNodePublishRequiresMountedStaging(t *testing.T) {
	d := &Driver{mounter: &mount.MockMounter{
		IsMountedFn: func(path string) (bool, error) {
			if path == "/staging" {
				return false, nil
			}
			t.Fatalf("unexpected IsMounted(%q) before the staging check failed", path)
			return false, nil
		},
	}}
	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "1234567",
		StagingTargetPath: "/staging",
		TargetPath:        "/target",
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		},
	})
	if err == nil {
		t.Fatal("expected FailedPrecondition for unmounted staging path, got success")
	}
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", err)
	}
}

// startFakeIMDS serves the two metadata values NewDriver's node init reads.
func startFakeIMDS(t *testing.T) { startFakeIMDSInZone(t, "KR-2") }

// startFakeIMDSInZone is startFakeIMDS with the node's zone code chosen by
// the caller, so tests can put the node in a zone the driver does not list.
func startFakeIMDSInZone(t *testing.T, zoneCode string) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/latest/api/token", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "test-token")
	})
	mux.HandleFunc("/latest/meta-data/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/meta-data/serverInstanceNo":
			fmt.Fprint(w, "200000002\n")
		case "/latest/meta-data/zoneCode":
			fmt.Fprint(w, zoneCode+"\n")
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Setenv("NCLOUD_METADATA_API_ENDPOINT", srv.URL)
}

// The attachment limit is never guessed: an explicit override skips the
// server spec lookup entirely, an API-reported limit is used as is, and a
// missing or unobtainable limit fails the startup with override guidance.
func TestNewDriverAttachLimitFailFast(t *testing.T) {
	startFakeIMDS(t)
	origAttempts, origDelay := startupRetryAttempts, startupRetryDelay
	startupRetryAttempts, startupRetryDelay = 2, time.Millisecond
	t.Cleanup(func() { startupRetryAttempts, startupRetryDelay = origAttempts, origDelay })

	nodeOpts := func() *Options {
		return &Options{Endpoint: "unix:///tmp/csi-test.sock", Mode: NodeMode, DefaultFsType: "ext4", RegionCode: "KR"}
	}

	t.Run("override skips the server spec lookup", func(t *testing.T) {
		t.Setenv(maxVolumesPerNodeEnv, "25")
		mc := &cloud.MockCloud{GetServerInstanceFn: func(_ context.Context, _ string) (*cloud.ServerInstance, error) {
			t.Fatal("GetServerInstance must not be called when the override is set")
			return nil, nil
		}}
		d, err := NewDriver(mc, &mount.MockMounter{}, nodeOpts())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.maxVolumesPerNode != 25 {
			t.Errorf("expected maxVolumesPerNode 25, got %d", d.maxVolumesPerNode)
		}
	})

	t.Run("API-reported limit is used", func(t *testing.T) {
		mc := &cloud.MockCloud{GetServerInstanceFn: func(_ context.Context, _ string) (*cloud.ServerInstance, error) {
			return &cloud.ServerInstance{MaxBlockStorageCount: 20}, nil
		}}
		d, err := NewDriver(mc, &mount.MockMounter{}, nodeOpts())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.maxVolumesPerNode != 20 {
			t.Errorf("expected maxVolumesPerNode 20, got %d", d.maxVolumesPerNode)
		}
	})

	t.Run("unreported limit fails startup with override guidance", func(t *testing.T) {
		mc := &cloud.MockCloud{GetServerInstanceFn: func(_ context.Context, _ string) (*cloud.ServerInstance, error) {
			return &cloud.ServerInstance{MaxBlockStorageCount: 0}, nil
		}}
		_, err := NewDriver(mc, &mount.MockMounter{}, nodeOpts())
		if err == nil || !strings.Contains(err.Error(), maxVolumesPerNodeEnv) {
			t.Fatalf("expected a failure naming %s, got %v", maxVolumesPerNodeEnv, err)
		}
	})

	t.Run("persistent API failure fails startup after retries", func(t *testing.T) {
		calls := 0
		mc := &cloud.MockCloud{GetServerInstanceFn: func(_ context.Context, _ string) (*cloud.ServerInstance, error) {
			calls++
			return nil, fmt.Errorf("gateway down")
		}}
		_, err := NewDriver(mc, &mount.MockMounter{}, nodeOpts())
		if err == nil {
			t.Fatal("expected startup failure, got nil")
		}
		if calls != startupRetryAttempts {
			t.Errorf("expected %d attempts, got %d", startupRetryAttempts, calls)
		}
	})
}

// The by-id symlink appears only after the hypervisor hot-plug; the attach
// paths poll for it instead of failing on the first miss.
func TestFindAbsoluteDeviceByIDPathWaitsForSymlink(t *testing.T) {
	newOpts := func(dir string, timeout time.Duration) deviceDiscoveryOptions {
		return deviceDiscoveryOptions{byIDDir: dir, interval: 10 * time.Millisecond, timeout: timeout}
	}

	t.Run("resolves a symlink that appears late", func(t *testing.T) {
		dir := t.TempDir()
		link := filepath.Join(dir, diskNcloudKVMPrefix+"9000042")
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = os.Symlink("/dev/null", link)
		}()
		resolved, err := findAbsoluteDeviceByIDPath(context.Background(), newOpts(dir, time.Second), diskNcloudKVMPrefix, "9000042")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resolved != "/dev/null" {
			t.Errorf("expected /dev/null, got %q", resolved)
		}
	})

	t.Run("times out when the symlink never appears", func(t *testing.T) {
		start := time.Now()
		_, err := findAbsoluteDeviceByIDPath(context.Background(), newOpts(t.TempDir(), 80*time.Millisecond), diskNcloudKVMPrefix, "9000042")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
			t.Errorf("returned before the timeout elapsed (%v)", elapsed)
		}
	})

	t.Run("timeout zero checks exactly once", func(t *testing.T) {
		start := time.Now()
		if _, err := findAbsoluteDeviceByIDPath(context.Background(), newOpts(t.TempDir(), 0), diskNcloudKVMPrefix, "9000042"); err == nil {
			t.Fatal("expected an error, got nil")
		}
		if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
			t.Errorf("single check took too long (%v)", elapsed)
		}
	})

	t.Run("request cancellation ends the wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(30 * time.Millisecond); cancel() }()
		_, err := findAbsoluteDeviceByIDPath(ctx, newOpts(t.TempDir(), time.Minute), diskNcloudKVMPrefix, "9000042")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
}

// A device-resolution failure is classified from the returned error, not
// from the request context: a malformed volume ID must not be reported as
// Canceled just because the request was cancelled by then.
func TestDeviceResolutionStatusClassifiesByError(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"cancelled wait", fmt.Errorf("device never appeared: %w", context.Canceled), codes.Canceled},
		{"deadline", fmt.Errorf("device never appeared: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{"malformed volume ID", errors.New(`volume ID "vol-1" is not a valid Ncloud block storage instance number`), codes.Internal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, _ := status.FromError(deviceResolutionStatus("9000001", tt.err))
			if st.Code() != tt.want {
				t.Errorf("expected %v, got %v", tt.want, st.Code())
			}
		})
	}
}

// The token-less fallback must not paper over an IMDS that is genuinely
// unavailable: a service that insists on the token answers 401 without
// one, so a failed token request still fails the read and the startup
// retry is what recovers.
func TestNewDriverRetriesWhenIMDSInsistsOnToken(t *testing.T) {
	var tokenAttempts int32
	mux := http.NewServeMux()
	mux.HandleFunc("/latest/api/token", func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&tokenAttempts, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "tok")
	})
	mux.HandleFunc("/latest/meta-data/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-NCP-METADATA-TOKEN") != "tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/latest/meta-data/serverInstanceNo":
			fmt.Fprintln(w, "200000002")
		case "/latest/meta-data/zoneCode":
			fmt.Fprintln(w, "KR-2")
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Setenv("NCLOUD_METADATA_API_ENDPOINT", srv.URL)

	origAttempts, origDelay := startupRetryAttempts, startupRetryDelay
	startupRetryAttempts, startupRetryDelay = 3, time.Millisecond
	t.Cleanup(func() { startupRetryAttempts, startupRetryDelay = origAttempts, origDelay })

	mc := &cloud.MockCloud{GetServerInstanceFn: func(_ context.Context, _ string) (*cloud.ServerInstance, error) {
		return &cloud.ServerInstance{MaxBlockStorageCount: 20}, nil
	}}
	d, err := NewDriver(mc, &mount.MockMounter{}, &Options{
		Endpoint: "unix:///tmp/csi-test.sock", Mode: NodeMode, DefaultFsType: "ext4", RegionCode: "KR",
	})
	if err != nil {
		t.Fatalf("startup should recover once the token endpoint returns: %v", err)
	}
	if d.serverInstanceNo != "200000002" {
		t.Errorf("serverInstanceNo = %q, want 200000002", d.serverInstanceNo)
	}
	if got := atomic.LoadInt32(&tokenAttempts); got < 2 {
		t.Errorf("expected the token request to be retried, got %d attempts", got)
	}
}
