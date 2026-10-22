// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
)

// legacyTopologyZoneKey is the topology key used by the original nks-csi
// driver. It is still parsed and advertised so StorageClasses and PVs
// created before the migration keep working. Deprecated: new configuration
// must use the standard topology.kubernetes.io/zone key.
const legacyTopologyZoneKey = "zoneNo"

// legacyTopologyRegionKey is the region topology key the original nks-csi
// driver advertised. PVs it provisioned carry BOTH regionNo and zoneNo in a
// single nodeSelectorTerm (ANDed), so nodes must keep exposing regionNo
// after migration or those PVs stop scheduling onto newly added nodes. The
// value is operator-supplied (--legacy-topology-region-no) because the
// numeric region number is not exposed by IMDS or the public SDK.
// Deprecated: removal requires a major version.
const legacyTopologyRegionKey = "regionNo"

// zoneFromSegments extracts a zone from topology segments, preferring the
// standard key and falling back to the legacy nks-csi key. The value is a
// zone number on a CCM-labeled cluster and may be a zone code elsewhere;
// either way it is normalized to the backend's zone code. ok is false when
// neither key carries a non-empty value. A value that names no zone this
// driver supports is an error rather than something to pass to the API.
func zoneFromSegments(segments map[string]string) (zoneCode string, ok bool, err error) {
	for _, key := range []string{corev1.LabelTopologyZone, legacyTopologyZoneKey} {
		raw := segments[key]
		if raw == "" {
			continue
		}
		code, err := cloud.ZoneCodeFromNo(raw)
		if err != nil {
			return "", false, fmt.Errorf("topology key %q: %w", key, err)
		}
		return code, true, nil
	}
	return "", false, nil
}

// selectPlacementZone picks the zone for a newly created volume from the
// accessibility requirements: the first valid Preferred topology wins
// (external-provisioner places the scheduler/rotation choice there),
// falling back to the first valid Requisite. An empty result with a nil
// error means the request carried no topology requirement and the backend
// default placement applies. A requirement whose entries carry none of the
// supported zone keys is rejected instead of being silently ignored.
func selectPlacementZone(req *csi.TopologyRequirement) (string, error) {
	if req == nil {
		return "", nil
	}
	topologies := append(append([]*csi.Topology{}, req.GetPreferred()...), req.GetRequisite()...)
	if len(topologies) == 0 {
		return "", nil
	}
	for _, topo := range topologies {
		zone, ok, err := zoneFromSegments(topo.GetSegments())
		if err != nil {
			return "", err
		}
		if ok {
			return zone, nil
		}
	}
	return "", fmt.Errorf("accessibility requirements carry no supported zone key (%s or %s)",
		corev1.LabelTopologyZone, legacyTopologyZoneKey)
}

// isZoneCompatible reports whether an existing volume's zone satisfies the
// topology requirement of a CreateVolume retry. Requisite is the hard
// constraint set; Preferred alone never disqualifies an existing volume
// because CSI defines it as a best-effort ordering, not a restriction.
func isZoneCompatible(req *csi.TopologyRequirement, existingZoneCode string) bool {
	requisite := req.GetRequisite()
	if len(requisite) == 0 {
		return true
	}
	for _, topo := range requisite {
		// A requisite naming an unsupported zone is skipped rather than
		// treated as a mismatch. It cannot name the existing volume's zone
		// -- the volume was created in a zone the table knows -- so
		// skipping and comparing give the same answer. Note that
		// selectPlacementZone returns at the first topology it can
		// resolve, so such a requisite may never have been examined.
		if zone, ok, err := zoneFromSegments(topo.GetSegments()); err == nil && ok && zone == existingZoneCode {
			return true
		}
	}
	return false
}
