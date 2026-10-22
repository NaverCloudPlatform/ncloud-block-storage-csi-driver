// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
//
// Derived from digitalocean/csi-digitalocean (Apache-2.0), forked at v0.4.1
// and merged up to v1.1.2 with later code ported from v1.2.0, Copyright
// 2018-2020 DigitalOcean, through the in-house nks-csi fork, and modified
// by NAVER Cloud Corp.

package driver

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

// Supported Ncloud block storage volume types. Two families, and the
// media follows the family rather than the number: Common Block Storage
// is SSD with IOPS that scale with the volume (100-16,000), Fixed Block
// Storage is HDD with a flat 500 IOPS. Read from
// getBlockStorageVolumeTypeList on the KR region.
const (
	VolumeTypeCB1 = "CB1" // Common Block Storage (SSD, 100-16,000 IOPS)
	VolumeTypeCB2 = "CB2" // Common Block Storage (SSD, 100-16,000 IOPS)
	VolumeTypeFB1 = "FB1" // Fixed Block Storage (HDD, 500 IOPS)
	VolumeTypeFB2 = "FB2" // Fixed Block Storage (HDD, 500 IOPS)
)

// volumeTypeSpec defines the size constraints for a volume type.
type volumeTypeSpec struct {
	MinSizeBytes int64
	MaxSizeBytes int64
}

// maxVolumeSizeBytes is the ceiling every type shares. The API reports
// 16,380 GiB — four short of a round 16 TiB — so a request for 16 TiB
// rounds up past the limit and the API rejects it; the driver answers
// OUT_OF_RANGE first, with the real number in the message.
const maxVolumeSizeBytes = 16380 * utils.GiB

// volumeTypeSpecs maps each supported volume type to its size constraints,
// as reported by getBlockStorageVolumeTypeList.
var volumeTypeSpecs = map[string]volumeTypeSpec{
	VolumeTypeCB1: {MinSizeBytes: 10 * utils.GiB, MaxSizeBytes: maxVolumeSizeBytes},
	VolumeTypeCB2: {MinSizeBytes: 10 * utils.GiB, MaxSizeBytes: maxVolumeSizeBytes},
	VolumeTypeFB1: {MinSizeBytes: 100 * utils.GiB, MaxSizeBytes: maxVolumeSizeBytes},
	VolumeTypeFB2: {MinSizeBytes: 100 * utils.GiB, MaxSizeBytes: maxVolumeSizeBytes},
}

// Ncloud block storage instance and snapshot names are limited to 30
// characters.
const maxVolumeNameLength = 30

// ncloudResourceName maps a CSI resource name onto the Ncloud name length
// limit. Names within the limit pass through unchanged — the shipped chart
// pins provisioner/snapshotter UUID lengths so real clusters always take
// this path. Longer names (CSI requires support for at least 128 bytes) are
// shortened deterministically to a 21-character prefix plus an 8-hex-digit
// SHA-256 tag: retries of the same request resolve to the same backend name,
// so idempotency-by-name lookup keeps working.
func ncloudResourceName(name string) string {
	if len(name) <= maxVolumeNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return fmt.Sprintf("%s-%x", name[:maxVolumeNameLength-9], sum[:4])
}

var supportedAccessMode = &csi.VolumeCapability_AccessMode{
	Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
}

func validVolumeTypeNames() []string {
	names := make([]string, 0, len(volumeTypeSpecs))
	for k := range volumeTypeSpecs {
		names = append(names, k)
	}
	return names
}

// paramVolumeType is the only StorageClass parameter this driver reads.
const paramVolumeType = "type"

// coParameterPrefix is the key namespace the CO reserves for itself.
// external-provisioner validates these keys and strips the ones it
// consumes (csi.storage.k8s.io/fstype, the secret references) before the
// request reaches the driver; what still arrives are the keys it adds
// with --extra-create-metadata (csi.storage.k8s.io/pvc/name,
// pvc/namespace, pv/name). The namespace passes through as a whole rather
// than being enumerated, so a provisioner that adds a key keeps working.
// The match is exact: a key that differs in case is not one the
// provisioner recognizes either, and would otherwise be ignored twice.
const coParameterPrefix = "csi.storage.k8s.io/"

// deprecatedCOParameters are the camel-case secret references that
// external-provisioner still accepts and does not strip from the
// parameters it forwards -- exactly these eight in v6.1.0 (read from the
// binary), matched exactly as the provisioner matches them. Letting every
// key that merely starts with "csi" through, as this check once did,
// turned a typo such as "csitype" into a silently ignored parameter.
var deprecatedCOParameters = map[string]struct{}{
	"csiProvisionerSecretName":            {},
	"csiProvisionerSecretNamespace":       {},
	"csiControllerPublishSecretName":      {},
	"csiControllerPublishSecretNamespace": {},
	"csiNodeStageSecretName":              {},
	"csiNodeStageSecretNamespace":         {},
	"csiNodePublishSecretName":            {},
	"csiNodePublishSecretNamespace":       {},
}

// legacyFsTypeParameter is the deprecated spelling of
// csi.storage.k8s.io/fstype. external-provisioner still reads it (case
// insensitively, with a deprecation warning) to pick the filesystem, and
// does not strip it from the parameters it forwards, so it arrives here
// on any StorageClass written before the prefixed key existed.
const legacyFsTypeParameter = "fstype"

// validateVolumeParameters rejects StorageClass parameters the driver does
// not understand. A silently ignored key is a trap: "Type: FB1" (wrong
// case) or a typo would provision the default type instead, and the
// mistake only shows up as unexpected cost or performance later. Keys the
// CO owns are none of the driver's business and pass through untouched --
// the exact namespace, not anything that happens to start with "csi".
func validateVolumeParameters(params map[string]string) error {
	var unknown []string
	for k := range params {
		if _, ok := deprecatedCOParameters[k]; ok {
			continue
		}
		lower := strings.ToLower(k)
		switch {
		case lower == paramVolumeType:
		case lower == legacyFsTypeParameter:
		case strings.HasPrefix(k, coParameterPrefix):
		default:
			unknown = append(unknown, k)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("unsupported StorageClass parameter(s) %v: this driver reads %q and passes through the keys Kubernetes owns (%s*, the deprecated csi*SecretName/csi*SecretNamespace spellings, and %q)",
		unknown, paramVolumeType, coParameterPrefix, legacyFsTypeParameter)
}

// volumeTypeParameter returns the requested volume type, or the default
// when the StorageClass does not set one. The key is matched case
// insensitively, the way the CO matches its own parameter names: a "Type:"
// that silently provisioned the default type was the mistake this whole
// check exists to stop, and honoring the request beats rejecting it. Two
// spellings carrying different values are a mistake rather than a choice,
// and picking one of them would depend on map iteration order.
func volumeTypeParameter(params map[string]string) (string, error) {
	volumeType := ""
	for k, v := range params {
		if !strings.EqualFold(k, paramVolumeType) {
			continue
		}
		v = strings.ToUpper(v)
		if volumeType != "" && v != volumeType {
			return "", fmt.Errorf("StorageClass sets %q more than once with conflicting values (%q and %q)",
				paramVolumeType, volumeType, v)
		}
		volumeType = v
	}
	if volumeType == "" {
		return DefaultVolumeType, nil
	}
	return volumeType, nil
}

// validateVolumeType checks whether the given volume type is supported by
// this driver. Everything outside the supported set is rejected uniformly —
// including the XEN-generation names (SSD/HDD), which the original nks-csi
// accepted by rewriting SSD to CB1 itself and passing HDD through to the
// backend. The same check applies to API-reported types: the public API
// reports SSD/HDD for first-generation (XEN) volumes, which a KVM-only
// driver can neither create nor resize (attach is not gated here).
func validateVolumeType(volumeType string) error {
	if _, ok := volumeTypeSpecs[volumeType]; !ok {
		return fmt.Errorf("unsupported volume type %q: supported types are %v", volumeType, validVolumeTypeNames())
	}
	return nil
}

// validateVolumeSizeForType checks whether the requested size is within the
// allowed range for the given volume type.
func validateVolumeSizeForType(volumeType string, sizeBytes int64) error {
	spec, ok := volumeTypeSpecs[volumeType]
	if !ok {
		return fmt.Errorf("unsupported volume type %q", volumeType)
	}
	if sizeBytes < spec.MinSizeBytes {
		return fmt.Errorf("requested size (%v) is below minimum (%v) for volume type %s",
			utils.FormatBytes(sizeBytes), utils.FormatBytes(spec.MinSizeBytes), volumeType)
	}
	if sizeBytes > spec.MaxSizeBytes {
		return fmt.Errorf("requested size (%v) exceeds maximum (%v) for volume type %s",
			utils.FormatBytes(sizeBytes), utils.FormatBytes(spec.MaxSizeBytes), volumeType)
	}
	return nil
}

// validateVolumeName guards the invariant that a name reaching the API fits
// the Ncloud limit. Callers shorten first (ncloudResourceName), so a failure
// here is a bug in the shortening, not a user error.
func validateVolumeName(name string) error {
	if len(name) > maxVolumeNameLength {
		return fmt.Errorf("volume name %q exceeds maximum length of %d characters", name, maxVolumeNameLength)
	}
	return nil
}

func validateCapabilities(caps []*csi.VolumeCapability) []string {
	var violations []string
	for _, cap := range caps {
		if cap.GetAccessMode().GetMode() != supportedAccessMode.GetMode() {
			violations = append(violations, fmt.Sprintf("unsupported access mode %s", cap.GetAccessMode().GetMode().String()))
		}

		accessType := cap.GetAccessType()
		switch accessType.(type) {
		case *csi.VolumeCapability_Block:
		case *csi.VolumeCapability_Mount:
		default:
			violations = append(violations, "unsupported access type")
		}
	}
	return violations
}
