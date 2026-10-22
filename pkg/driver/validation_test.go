// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

func TestValidateVolumeType(t *testing.T) {
	tests := []struct {
		name       string
		volumeType string
		wantErr    bool
	}{
		{name: "CB1 valid", volumeType: "CB1"},
		{name: "CB2 valid", volumeType: "CB2"},
		{name: "FB1 valid", volumeType: "FB1"},
		{name: "FB2 valid", volumeType: "FB2"},
		{name: "unknown type", volumeType: "UNKNOWN", wantErr: true},
		{name: "empty type", volumeType: "", wantErr: true},
		{name: "lowercase rejected", volumeType: "cb1", wantErr: true},
		// The XEN-generation names were once aliased (SSD) or passed
		// through (HDD) by the original driver; both must reject like any
		// other unsupported type.
		{name: "XEN-generation SSD rejected", volumeType: "SSD", wantErr: true},
		{name: "XEN-generation HDD rejected", volumeType: "HDD", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVolumeType(tt.volumeType)
			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateVolumeSizeForType(t *testing.T) {
	tests := []struct {
		name       string
		volumeType string
		sizeBytes  int64
		wantErr    bool
	}{
		{name: "CB1 at minimum", volumeType: "CB1", sizeBytes: 10 * utils.GiB},
		{name: "CB1 below minimum", volumeType: "CB1", sizeBytes: 5 * utils.GiB, wantErr: true},
		{name: "CB1 at maximum", volumeType: "CB1", sizeBytes: maxVolumeSizeBytes},
		{name: "CB1 above maximum", volumeType: "CB1", sizeBytes: 17 * utils.TiB, wantErr: true},
		// The ceiling is 16,380 GiB, four short of a round 16 TiB: a
		// request for 16 TiB is over the limit, not at it.
		{name: "CB1 at a round 16 TiB is over the ceiling", volumeType: "CB1", sizeBytes: 16 * utils.TiB, wantErr: true},
		{name: "FB1 at maximum", volumeType: "FB1", sizeBytes: maxVolumeSizeBytes},
		{name: "FB1 at minimum", volumeType: "FB1", sizeBytes: 100 * utils.GiB},
		{name: "FB1 below minimum", volumeType: "FB1", sizeBytes: 50 * utils.GiB, wantErr: true},
		{name: "FB2 at minimum", volumeType: "FB2", sizeBytes: 100 * utils.GiB},
		{name: "FB2 at 10GiB", volumeType: "FB2", sizeBytes: 10 * utils.GiB, wantErr: true},
		{name: "unknown type", volumeType: "INVALID", sizeBytes: 100 * utils.GiB, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVolumeSizeForType(tt.volumeType, tt.sizeBytes)
			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateVolumeName(t *testing.T) {
	tests := []struct {
		name    string
		volName string
		wantErr bool
	}{
		{name: "short name", volName: "my-vol"},
		{name: "at limit", volName: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "exceeds limit", volName: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVolumeName(tt.volName)
			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestNcloudResourceName(t *testing.T) {
	atLimit := strings.Repeat("a", maxVolumeNameLength)
	over := strings.Repeat("b", maxVolumeNameLength+1)
	long1 := strings.Repeat("c", 100) + "1"
	long2 := strings.Repeat("c", 100) + "2"

	if got := ncloudResourceName("my-vol"); got != "my-vol" {
		t.Errorf("short name changed: %q", got)
	}
	if got := ncloudResourceName(atLimit); got != atLimit {
		t.Errorf("at-limit name changed: %q", got)
	}

	short := ncloudResourceName(over)
	if len(short) != maxVolumeNameLength {
		t.Errorf("shortened name has length %d, want %d: %q", len(short), maxVolumeNameLength, short)
	}
	if !strings.HasPrefix(short, over[:maxVolumeNameLength-9]) {
		t.Errorf("shortened name %q does not keep the original prefix", short)
	}
	if again := ncloudResourceName(over); again != short {
		t.Errorf("shortening is not deterministic: %q vs %q", short, again)
	}
	if s1, s2 := ncloudResourceName(long1), ncloudResourceName(long2); s1 == s2 {
		t.Errorf("names differing past the prefix collided: %q", s1)
	}
}

func TestValidateCapabilities(t *testing.T) {
	tests := []struct {
		name           string
		caps           []*csi.VolumeCapability
		wantViolations int
	}{
		{
			name: "valid - single node writer mount",
			caps: []*csi.VolumeCapability{
				{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				},
			},
			wantViolations: 0,
		},
		{
			name: "valid - single node writer block",
			caps: []*csi.VolumeCapability{
				{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
					AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
				},
			},
			wantViolations: 0,
		},
		{
			name: "invalid - multi node multi writer",
			caps: []*csi.VolumeCapability{
				{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				},
			},
			wantViolations: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			violations := validateCapabilities(tt.caps)
			if len(violations) != tt.wantViolations {
				t.Errorf("expected %d violations, got %d: %v", tt.wantViolations, len(violations), violations)
			}
		})
	}
}
