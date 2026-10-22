// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
//
// Derived from digitalocean/csi-digitalocean (Apache-2.0), forked at v0.4.1
// and merged up to v1.1.2 with later code ported from v1.2.0, Copyright
// 2018-2020 DigitalOcean, through the in-house nks-csi fork, and modified
// by NAVER Cloud Corp.

// Package utils provides common utility functions and constants for the
// Ncloud Block Storage CSI Driver.
package utils

import (
	"fmt"
	"math"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// Byte size unit constants.
const (
	_   = iota
	KiB = 1 << (10 * iota)
	MiB
	GiB
	TiB
)

// Ncloud block storage allocation constraints.
const (
	// NcloudVolumeSizeUnitInBytes is the allocation unit for Ncloud block storage.
	// All volume sizes must be a multiple of this value.
	NcloudVolumeSizeUnitInBytes int64 = 10 * GiB
	DefaultVolumeSizeInBytes    int64 = 20 * GiB
)

// ExtractStorage parses the capacity range and returns the requested size
// in bytes. Alignment and type-specific validation are handled separately
// by the driver layer.
func ExtractStorage(capRange *csi.CapacityRange) (int64, error) {
	if capRange == nil {
		return DefaultVolumeSizeInBytes, nil
	}

	requiredBytes := capRange.GetRequiredBytes()
	limitBytes := capRange.GetLimitBytes()

	if requiredBytes < 0 || limitBytes < 0 {
		return 0, fmt.Errorf("capacity range values must not be negative (required=%d, limit=%d)", requiredBytes, limitBytes)
	}

	requiredSet := requiredBytes > 0
	limitSet := limitBytes > 0

	// CSI requires at least one of the fields to be set when a capacity
	// range message is present.
	if !requiredSet && !limitSet {
		return 0, fmt.Errorf("capacity range must set at least one of required_bytes or limit_bytes")
	}

	if requiredSet && limitSet && limitBytes < requiredBytes {
		return 0, fmt.Errorf("limit (%v) can not be less than required (%v) size", FormatBytes(limitBytes), FormatBytes(requiredBytes))
	}

	if requiredSet {
		return requiredBytes, nil
	}

	return limitBytes, nil
}

// FormatBytes renders a byte count with binary unit suffixes (Ki, Mi, Gi,
// Ti), choosing the largest unit that divides it exactly.
//
// Every caller builds a message comparing one size against another -- a
// request against a limit, a volume against its snapshot -- so two
// different sizes must never render as the same text. Rounding to a fixed
// number of decimals breaks that: the 16,380 GiB volume ceiling and the
// 16 TiB request it rejects both come out "16Ti", and the message reads
// as a contradiction. Picking an exact unit keeps every rendering
// truthful, at the cost of an occasional long one (102912Mi for a size
// that is not a whole number of GiB).
func FormatBytes(inputBytes int64) string {
	if inputBytes == 0 {
		return "0"
	}

	for _, u := range []struct {
		size int64
		name string
	}{{TiB, "Ti"}, {GiB, "Gi"}, {MiB, "Mi"}, {KiB, "Ki"}} {
		if inputBytes%u.size == 0 {
			return strconv.FormatInt(inputBytes/u.size, 10) + u.name
		}
	}

	return strconv.FormatInt(inputBytes, 10)
}

// RoundUpBytes rounds sizeBytes up to the nearest Ncloud block storage
// allocation unit (10 GiB). This avoids rejecting valid Kubernetes capacity
// requests that do not fall exactly on the boundary. Values that would
// overflow round up to MaxInt64 instead of wrapping negative; type-specific
// maximum validation rejects them downstream.
func RoundUpBytes(sizeBytes int64) int64 {
	unit := NcloudVolumeSizeUnitInBytes
	units := sizeBytes / unit
	if sizeBytes%unit != 0 {
		units++
	}
	if units > math.MaxInt64/unit {
		return math.MaxInt64
	}
	return units * unit
}

// RoundDownBytes rounds sizeBytes down to the nearest Ncloud block storage
// allocation unit (10 GiB). Used for limit-only capacity ranges, where the
// limit is a ceiling: rounding up would overshoot every non-aligned limit
// and reject requests that are perfectly satisfiable. Returns 0 when
// sizeBytes is below one allocation unit.
func RoundDownBytes(sizeBytes int64) int64 {
	return (sizeBytes / NcloudVolumeSizeUnitInBytes) * NcloudVolumeSizeUnitInBytes
}
