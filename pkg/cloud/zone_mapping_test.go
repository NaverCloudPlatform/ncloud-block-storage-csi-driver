// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package cloud

import (
	"strings"
	"testing"
)

func TestZoneNoFromCode(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
	}{
		{"KR-1", "2"},
		{"KR-2", "3"},
		{"KR-3", "4"},
		{"SGN-1", "70"},
		{"SGN-4", "74"},
		{"SGN-5", "75"},
		{"JPN-1", "80"},
		{"JPN-4", "84"},
		{"JPN-5", "85"},
		{"USWN-1", "90"},
		{"DEN-1", "100"},
		// Financial and government platforms share the numbering.
		{"FKR-1", "110"},
		{"FKR-2", "130"},
		{"KRS-1", "135"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ZoneNoFromCode(tt.input)
			if err != nil {
				t.Fatalf("ZoneNoFromCode(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ZoneNoFromCode(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// A zone the table does not list has no number the driver could advertise,
// and guessing one collides with the label the cloud controller manager
// applies. The error names the supported zones so the operator can see
// whether the zone needs a driver release.
func TestZoneNoFromCodeRejectsUnknownZones(t *testing.T) {
	for _, code := range []string{"UNKNOWN-1", "", "kr-1", "2", "PCS-1"} {
		got, err := ZoneNoFromCode(code)
		if err == nil {
			t.Errorf("ZoneNoFromCode(%q) = %q, want an error", code, got)
			continue
		}
		if !strings.Contains(err.Error(), "KR-1=2") {
			t.Errorf("ZoneNoFromCode(%q) error does not name the supported zones: %v", code, err)
		}
	}
}

func TestZoneCodeFromNo(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
	}{
		{"2", "KR-1"},
		{"3", "KR-2"},
		{"4", "KR-3"},
		{"70", "SGN-1"},
		{"74", "SGN-4"},
		{"75", "SGN-5"},
		{"80", "JPN-1"},
		{"84", "JPN-4"},
		{"85", "JPN-5"},
		{"90", "USWN-1"},
		{"100", "DEN-1"},
		{"110", "FKR-1"},
		{"130", "FKR-2"},
		{"135", "KRS-1"},
		// A zone code passes through: a cluster without the CCM label
		// carries whatever the driver itself advertised.
		{"KR-1", "KR-1"},
		{"KRS-1", "KRS-1"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ZoneCodeFromNo(tt.input)
			if err != nil {
				t.Fatalf("ZoneCodeFromNo(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ZoneCodeFromNo(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestZoneCodeFromNoRejectsUnknownZones(t *testing.T) {
	for _, zone := range []string{"999", "", "UNKNOWN-1", "PCS-1"} {
		if got, err := ZoneCodeFromNo(zone); err == nil {
			t.Errorf("ZoneCodeFromNo(%q) = %q, want an error", zone, got)
		}
	}
}

func TestBidirectionalConsistency(t *testing.T) {
	for code, no := range zoneCodeToNo {
		got, err := ZoneCodeFromNo(no)
		if err != nil || got != code {
			t.Errorf("ZoneCodeFromNo(%q) = %q, %v; want %q", no, got, err, code)
		}
		got, err = ZoneNoFromCode(code)
		if err != nil || got != no {
			t.Errorf("ZoneNoFromCode(%q) = %q, %v; want %q", code, got, err, no)
		}
	}
}

// The listing is ordered by zone number so the message reads like the
// platform's own zone list rather than Go's map order.
func TestSupportedZonesIsOrderedAndComplete(t *testing.T) {
	got := SupportedZones()
	if !strings.HasPrefix(got, "KR-1=2, KR-2=3, KR-3=4, ") {
		t.Errorf("SupportedZones() does not start with the Korean public zones: %q", got)
	}
	if !strings.HasSuffix(got, "FKR-1=110, FKR-2=130, KRS-1=135") {
		t.Errorf("SupportedZones() does not end with the financial and government zones: %q", got)
	}
	if n := strings.Count(got, "="); n != len(zoneCodeToNo) {
		t.Errorf("SupportedZones() lists %d zones, want %d", n, len(zoneCodeToNo))
	}
}

func TestRegionCodeFromZone(t *testing.T) {
	for _, tt := range []struct{ zone, want string }{
		{"KR-2", "KR"},
		{"SGN-4", "SGN"},
		{"USWN-1", "USWN"},
		{"FKR-1", "FKR"},
		{"KRS-1", "KRS"},
		// No region prefix to take.
		{"KR", ""},
		{"", ""},
		{"-1", ""},
	} {
		if got := RegionCodeFromZone(tt.zone); got != tt.want {
			t.Errorf("RegionCodeFromZone(%q) = %q, want %q", tt.zone, got, tt.want)
		}
	}
}

// The check is only safe to fail startup on because every zone the driver
// supports carries its own region as the prefix the API expects, so the
// table itself is the first thing asserted here.
func TestValidateRegionForZone(t *testing.T) {
	for code := range zoneCodeToNo {
		if err := ValidateRegionForZone(RegionCodeFromZone(code), code); err != nil {
			t.Errorf("zone %s: unexpected error: %v", code, err)
		}
	}

	for _, tt := range []struct{ region, zone string }{
		{"KR", "KR-2"},
		// The API accepts either casing for the region.
		{"kr", "KR-2"},
		{"FKR", "FKR-1"},
		{"KRS", "KRS-1"},
		{"USWN", "USWN-1"},
	} {
		if err := ValidateRegionForZone(tt.region, tt.zone); err != nil {
			t.Errorf("region %q zone %q: unexpected error: %v", tt.region, tt.zone, err)
		}
	}

	for _, tt := range []struct{ region, zone string }{
		{"SGN", "KR-2"},
		// The financial and government regions are not the public one, even
		// though their zone numbers come from the same sequence.
		{"KR", "FKR-1"},
		{"KR", "KRS-1"},
		{"", "KR-2"},
		{"KR", "KR"},
		{"KR", ""},
	} {
		if err := ValidateRegionForZone(tt.region, tt.zone); err == nil {
			t.Errorf("region %q zone %q: expected an error", tt.region, tt.zone)
		}
	}
}
