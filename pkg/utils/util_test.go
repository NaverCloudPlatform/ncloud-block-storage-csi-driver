// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package utils

import (
	"math"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestRoundUpBytesOverflow(t *testing.T) {
	if got := RoundUpBytes(15 * GiB); got != 20*GiB {
		t.Errorf("expected 20GiB, got %d", got)
	}
	if got := RoundUpBytes(math.MaxInt64); got < 0 {
		t.Errorf("round-up of MaxInt64 must not wrap negative, got %d", got)
	}
	if got := RoundUpBytes(math.MaxInt64 - NcloudVolumeSizeUnitInBytes); got < 0 {
		t.Errorf("round-up near MaxInt64 must not wrap negative, got %d", got)
	}
}

func TestExtractStorage(t *testing.T) {
	tests := []struct {
		name     string
		capRange *csi.CapacityRange
		wantSize int64
		wantErr  bool
	}{
		{
			name:     "nil range returns default",
			capRange: nil,
			wantSize: DefaultVolumeSizeInBytes,
		},
		{
			name:     "non-nil range with both fields zero is rejected",
			capRange: &csi.CapacityRange{},
			wantErr:  true,
		},
		{
			name:     "negative required bytes is rejected",
			capRange: &csi.CapacityRange{RequiredBytes: -1},
			wantErr:  true,
		},
		{
			name:     "negative limit bytes is rejected",
			capRange: &csi.CapacityRange{RequiredBytes: 10 * GiB, LimitBytes: -1},
			wantErr:  true,
		},
		{
			name:     "exact 20GiB",
			capRange: &csi.CapacityRange{RequiredBytes: 20 * GiB},
			wantSize: 20 * GiB,
		},
		{
			name:     "returns raw required bytes without rounding",
			capRange: &csi.CapacityRange{RequiredBytes: 15 * GiB},
			wantSize: 15 * GiB,
		},
		{
			name:     "limit only returns limit bytes",
			capRange: &csi.CapacityRange{LimitBytes: 30 * GiB},
			wantSize: 30 * GiB,
		},
		{
			name:     "required within limit returns required",
			capRange: &csi.CapacityRange{RequiredBytes: 20 * GiB, LimitBytes: 50 * GiB},
			wantSize: 20 * GiB,
		},
		{
			name:     "limit less than required - error",
			capRange: &csi.CapacityRange{RequiredBytes: 30 * GiB, LimitBytes: 20 * GiB},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, err := ExtractStorage(tt.capRange)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != tt.wantSize {
				t.Errorf("expected %d, got %d", tt.wantSize, size)
			}
		})
	}
}

// The sizes FormatBytes renders are almost always two halves of a
// comparison ("requested X exceeds maximum Y"), so distinct byte counts
// must produce distinct text. The volume ceiling is the case that caught
// this: 16,380 GiB and the 16 TiB request it rejects both rounded to
// "16Ti", making the rejection read as a contradiction.
func TestFormatBytes(t *testing.T) {
	for _, tt := range []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{10 * GiB, "10Gi"},
		{16 * TiB, "16Ti"},
		{16380 * GiB, "16380Gi"},
		{1536 * MiB, "1536Mi"},
		{4 * KiB, "4Ki"},
		{12345, "12345"},
	} {
		if got := FormatBytes(tt.in); got != tt.want {
			t.Errorf("FormatBytes(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}

	if ceiling, request := FormatBytes(16380*GiB), FormatBytes(16*TiB); ceiling == request {
		t.Errorf("the volume ceiling and a 16 TiB request both render as %q", ceiling)
	}
}
