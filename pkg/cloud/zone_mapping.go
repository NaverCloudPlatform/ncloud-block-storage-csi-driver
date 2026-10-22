// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package cloud

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// zoneCodeToNo maps the zone codes of the Ncloud zones this driver supports
// to their zone numbers.
//
// Two vocabularies name a zone. The block storage API and the instance
// metadata service use zone codes ("KR-2"); the Ncloud cloud controller
// manager labels nodes with the zone number ("3") in
// topology.kubernetes.io/zone. No public API exposes the number, so the
// driver carries this table: the node plugin advertises the number (any
// other value makes the kubelet reject the driver with a topology value
// collision) and the controller turns the numbers it receives back into
// zone codes for the API. The numbering is one sequence shared by the
// public, financial and government platforms; the government platform's
// KR-1 and KR-2 use the public numbers.
//
// A zone that is not listed is refused at startup and in CreateVolume;
// supporting a new zone is a driver release.
var zoneCodeToNo = map[string]string{
	// Public platform
	"KR-1":   "2",
	"KR-2":   "3",
	"KR-3":   "4",
	"SGN-1":  "70",
	"SGN-4":  "74",
	"SGN-5":  "75",
	"JPN-1":  "80",
	"JPN-4":  "84",
	"JPN-5":  "85",
	"USWN-1": "90",
	"DEN-1":  "100",
	// Financial platform
	"FKR-1": "110",
	"FKR-2": "130",
	// Government platform (its KR-1 and KR-2 are the rows above)
	"KRS-1": "135",
}

// zoneNoToCode is the reverse mapping, built from zoneCodeToNo.
var zoneNoToCode = func() map[string]string {
	m := make(map[string]string, len(zoneCodeToNo))
	for code, no := range zoneCodeToNo {
		m[no] = code
	}
	return m
}()

// ZoneNoFromCode returns the zone number of zoneCode ("KR-2" -> "3"), or an
// error naming the supported zones when the zone is not in the table.
func ZoneNoFromCode(zoneCode string) (string, error) {
	if no, ok := zoneCodeToNo[zoneCode]; ok {
		return no, nil
	}
	return "", fmt.Errorf("zone code %q is not a zone this driver supports (%s)", zoneCode, SupportedZones())
}

// ZoneCodeFromNo returns the zone code of zone, which may be a zone number
// ("3") or already a zone code ("KR-2"), or an error naming the supported
// zones when it is neither.
func ZoneCodeFromNo(zone string) (string, error) {
	if code, ok := zoneNoToCode[zone]; ok {
		return code, nil
	}
	if _, ok := zoneCodeToNo[zone]; ok {
		return zone, nil
	}
	return "", fmt.Errorf("zone %q is neither a zone number nor a zone code this driver supports (%s)", zone, SupportedZones())
}

// RegionCodeFromZone returns the region code a zone code belongs to ("KR"
// for "KR-1", "SGN" for "SGN-4"), or "" when the zone code has no region
// prefix.
func RegionCodeFromZone(zoneCode string) string {
	if i := strings.LastIndex(zoneCode, "-"); i > 0 {
		return zoneCode[:i]
	}
	return ""
}

// ValidateRegionForZone checks that regionCode, the region the driver sends
// to the block storage API, is the region of zoneCode. Every zone code of
// the table carries its region before the last '-', and that prefix is the
// regionCode the platform API expects (KR, SGN, JPN, USWN, DEN, FKR, KRS).
// A block volume can only be attached to a server in its own zone, so the
// API region has to be this node's: any other value is a configuration
// error whose symptoms show up far from the cause -- volumes this node
// created going missing on the next lookup, attach failing on a volume the
// console shows as present.
func ValidateRegionForZone(regionCode, zoneCode string) error {
	region := RegionCodeFromZone(zoneCode)
	if region == "" {
		return fmt.Errorf("zone code %q carries no region", zoneCode)
	}
	if !strings.EqualFold(region, regionCode) {
		return fmt.Errorf("regionCode %q is not the region of zone %q (%s)", regionCode, zoneCode, region)
	}
	return nil
}

// SupportedZones lists the supported zones as "KR-1=2, KR-2=3, ..." ordered
// by zone number, for error messages and logs.
func SupportedZones() string {
	codes := make([]string, 0, len(zoneCodeToNo))
	for code := range zoneCodeToNo {
		codes = append(codes, code)
	}
	sort.Slice(codes, func(i, j int) bool {
		a, _ := strconv.Atoi(zoneCodeToNo[codes[i]])
		b, _ := strconv.Atoi(zoneCodeToNo[codes[j]])
		return a < b
	})
	parts := make([]string, len(codes))
	for i, code := range codes {
		parts[i] = code + "=" + zoneCodeToNo[code]
	}
	return strings.Join(parts, ", ")
}
