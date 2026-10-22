// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import "sync"

// inFlight tracks the resources an RPC is currently working on: volumes
// by ID on the node side (Driver.inFlight); volumes and snapshots by ID
// and, for a create, the Ncloud name on the controller side
// (Driver.controllerInFlight). The node case is the one described here;
// the controller's reasons are with its field.
//
// The node RPCs are not instant: NodeStageVolume can run mkfs on a fresh
// device, and the mount helpers carry a two-minute deadline, which is
// also how long the kubelet waits before it gives up and calls again.
// The second call would then format or mount a device the first call is
// still formatting. CSI allows the plugin to answer ABORTED for a
// concurrent request on the same volume and have the CO retry, which is
// what the key here is for.
//
// On the node side the key is the volume ID rather than the target path:
// the operations that collide (format, mount, unmount of the staging
// device) are properties of the volume, and a publish and a stage for the
// same volume are not safe to interleave either.
//
// The zero value is ready to use. That matters because a Driver is built
// directly in tests as well as through NewDriver, and a guard that only
// exists on one of those paths is a guard that is not tested.
type inFlight struct {
	mu     sync.Mutex
	active map[string]struct{}
}

// acquire claims the volume and reports whether the claim succeeded. A
// false return means another RPC holds it.
func (f *inFlight) acquire(volumeID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, busy := f.active[volumeID]; busy {
		return false
	}
	if f.active == nil {
		f.active = make(map[string]struct{})
	}
	f.active[volumeID] = struct{}{}
	return true
}

// release gives the volume back. Releasing one that is not held is a
// no-op, so a deferred release is always safe to write.
func (f *inFlight) release(volumeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.active, volumeID)
}
