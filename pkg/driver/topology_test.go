// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestZoneFromSegments(t *testing.T) {
	tests := []struct {
		name     string
		segments map[string]string
		wantZone string
		wantOK   bool
		wantErr  bool
	}{
		{
			name:     "standard key with zoneNo value",
			segments: map[string]string{"topology.kubernetes.io/zone": "2"},
			wantZone: "KR-1",
			wantOK:   true,
		},
		{
			name:     "standard key with zoneCode value passes through",
			segments: map[string]string{"topology.kubernetes.io/zone": "KR-2"},
			wantZone: "KR-2",
			wantOK:   true,
		},
		{
			name:     "legacy zoneNo key",
			segments: map[string]string{"zoneNo": "3"},
			wantZone: "KR-2",
			wantOK:   true,
		},
		{
			name: "standard key wins over legacy key",
			segments: map[string]string{
				"topology.kubernetes.io/zone": "2",
				"zoneNo":                      "3",
			},
			wantZone: "KR-1",
			wantOK:   true,
		},
		{
			name:     "no supported key",
			segments: map[string]string{"kubernetes.io/hostname": "node-1"},
			wantOK:   false,
		},
		{
			name:     "empty value is ignored",
			segments: map[string]string{"topology.kubernetes.io/zone": ""},
			wantOK:   false,
		},
		{
			// A number or code outside the table names a zone the driver
			// cannot reach; passing it to the API would fail there instead.
			name:     "zone outside the table is rejected",
			segments: map[string]string{"topology.kubernetes.io/zone": "999"},
			wantErr:  true,
		},
		{
			name:     "unsupported zone code is rejected",
			segments: map[string]string{"zoneNo": "PCS-1"},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zone, ok, err := zoneFromSegments(tt.segments)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got zone %q ok=%v", zone, ok)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.wantOK {
				t.Fatalf("expected ok=%v, got %v", tt.wantOK, ok)
			}
			if ok && zone != tt.wantZone {
				t.Errorf("expected zone %q, got %q", tt.wantZone, zone)
			}
		})
	}
}

func TestSelectPlacementZone(t *testing.T) {
	tests := []struct {
		name     string
		req      *csi.TopologyRequirement
		wantZone string
		wantErr  bool
	}{
		{
			name:     "nil requirement means backend default",
			req:      nil,
			wantZone: "",
		},
		{
			name:     "empty requirement means backend default",
			req:      &csi.TopologyRequirement{},
			wantZone: "",
		},
		{
			name: "first preferred wins",
			req: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "2"}},
					{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
				},
				Preferred: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
					{Segments: map[string]string{"topology.kubernetes.io/zone": "2"}},
				},
			},
			wantZone: "KR-2",
		},
		{
			name: "requisite-only falls back to first requisite",
			req: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "2"}},
				},
			},
			wantZone: "KR-1",
		},
		{
			name: "unparseable preferred falls back to requisite",
			req: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
				},
				Preferred: []*csi.Topology{
					{Segments: map[string]string{"kubernetes.io/hostname": "node-1"}},
				},
			},
			wantZone: "KR-2",
		},
		{
			name: "requirement without any supported key is rejected",
			req: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"kubernetes.io/hostname": "node-1"}},
				},
			},
			wantErr: true,
		},
		{
			// Refused here rather than at the API: the driver has no zone
			// code to send for a number it does not know.
			name: "zone number outside the table is rejected",
			req: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "999"}},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zone, err := selectPlacementZone(tt.req)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if zone != tt.wantZone {
				t.Errorf("expected zone %q, got %q", tt.wantZone, zone)
			}
		})
	}
}

func TestIsZoneCompatible(t *testing.T) {
	tests := []struct {
		name         string
		req          *csi.TopologyRequirement
		existingZone string
		want         bool
	}{
		{
			name:         "nil requirement is always compatible",
			req:          nil,
			existingZone: "KR-1",
			want:         true,
		},
		{
			name: "preferred-only mismatch stays compatible",
			req: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
				},
			},
			existingZone: "KR-1",
			want:         true,
		},
		{
			name: "zone inside requisite is compatible",
			req: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "2"}},
					{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
				},
			},
			existingZone: "KR-1",
			want:         true,
		},
		{
			name: "zone outside requisite is incompatible",
			req: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.kubernetes.io/zone": "3"}},
				},
			},
			existingZone: "KR-1",
			want:         false,
		},
		{
			name: "legacy zoneNo requisite matches",
			req: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"zoneNo": "2"}},
				},
			},
			existingZone: "KR-1",
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isZoneCompatible(tt.req, tt.existingZone); got != tt.want {
				t.Errorf("expected %v, got %v", tt.want, got)
			}
		})
	}
}
