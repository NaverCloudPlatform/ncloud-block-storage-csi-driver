// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package cloud

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

// maxResourceNameLength mirrors the Ncloud API limit on block storage and
// snapshot names. FakeCloud enforces it so a driver path that forgets to
// shorten a long CSI name fails in tests the same way it would against the
// real API.
const maxResourceNameLength = 30

// FakeCloud is an in-memory, stateful Cloud implementation for tests such as
// the csi-sanity suite. Unlike MockCloud (per-call function stubs), it models
// the observable behavior of the Ncloud backend: name-based lookup,
// create→attach→detach status transitions, snapshot lineage, and name length
// limits. All waits complete immediately because state transitions are
// synchronous.
type FakeCloud struct {
	mu        sync.Mutex
	volumes   map[string]*Volume
	snapshots map[string]*Snapshot
	servers   map[string]*ServerInstance
	nextID    int

	// DefaultZoneCode is used for volumes created without an explicit zone,
	// mimicking the backend's default placement.
	DefaultZoneCode string
}

var _ Cloud = &FakeCloud{}

// NewFakeCloud returns a FakeCloud placing zoneless volumes in defaultZone
// and knowing the given server instances.
func NewFakeCloud(defaultZone string, servers ...*ServerInstance) *FakeCloud {
	f := &FakeCloud{
		volumes:         map[string]*Volume{},
		snapshots:       map[string]*Snapshot{},
		servers:         map[string]*ServerInstance{},
		nextID:          9000001,
		DefaultZoneCode: defaultZone,
	}
	for _, s := range servers {
		f.servers[s.ServerInstanceNo] = s
	}
	return f
}

func (f *FakeCloud) newID() string {
	id := strconv.Itoa(f.nextID)
	f.nextID++
	return id
}

func copyVolume(v *Volume) *Volume {
	c := *v
	return &c
}

func copySnapshot(s *Snapshot) *Snapshot {
	c := *s
	return &c
}

func (f *FakeCloud) CreateVolume(_ context.Context, opts *VolumeOptions) (*Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(opts.Name) > maxResourceNameLength {
		return nil, fmt.Errorf("block storage name %q exceeds %d characters", opts.Name, maxResourceNameLength)
	}
	for _, v := range f.volumes {
		if v.Name == opts.Name {
			return nil, fmt.Errorf("block storage name %q already in use: %w", opts.Name, ErrNameInUse)
		}
	}
	if opts.SnapshotID != "" {
		if _, ok := f.snapshots[opts.SnapshotID]; !ok {
			return nil, fmt.Errorf("source snapshot %q not found", opts.SnapshotID)
		}
	}

	zone := opts.ZoneCode
	if zone == "" {
		zone = f.DefaultZoneCode
	}
	vol := &Volume{
		ID:             f.newID(),
		Name:           opts.Name,
		SizeBytes:      int64(opts.SizeGiB) * utils.GiB,
		StatusCode:     StatusCreated,
		OperationCode:  OpNormal,
		VolumeTypeCode: opts.VolumeType,
		ZoneCode:       zone,
	}
	f.volumes[vol.ID] = vol
	return copyVolume(vol), nil
}

func (f *FakeCloud) DeleteVolume(_ context.Context, volumeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	vol, ok := f.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}
	if vol.ServerInstanceNo != "" {
		return fmt.Errorf("block storage %q is attached to server %q and cannot be deleted", volumeID, vol.ServerInstanceNo)
	}
	delete(f.volumes, volumeID)
	return nil
}

func (f *FakeCloud) GetVolumeByID(_ context.Context, volumeID string) (*Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	vol, ok := f.volumes[volumeID]
	if !ok {
		return nil, ErrVolumeNotFound
	}
	return copyVolume(vol), nil
}

func (f *FakeCloud) GetVolumeByName(_ context.Context, name string) (*Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, vol := range f.volumes {
		if vol.Name == name {
			return copyVolume(vol), nil
		}
	}
	return nil, ErrVolumeNotFound
}

func (f *FakeCloud) ListVolumes(_ context.Context) ([]*Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]*Volume, 0, len(f.volumes))
	for _, vol := range f.volumes {
		out = append(out, copyVolume(vol))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *FakeCloud) AttachVolume(_ context.Context, volumeID, serverInstanceNo string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	vol, ok := f.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}
	srv, ok := f.servers[serverInstanceNo]
	if !ok {
		return fmt.Errorf("get server instance %q: %w", serverInstanceNo, ErrServerInstanceNotFound)
	}
	if vol.ServerInstanceNo != "" && vol.ServerInstanceNo != serverInstanceNo {
		return fmt.Errorf("block storage %q is already attached to server %q", volumeID, vol.ServerInstanceNo)
	}
	if vol.ServerInstanceNo == serverInstanceNo {
		return nil // already attached
	}
	if limit := srv.MaxBlockStorageCount; limit > 0 {
		attached := int32(0)
		for _, v := range f.volumes {
			if v.ServerInstanceNo == serverInstanceNo {
				attached++
			}
		}
		if attached >= limit {
			return fmt.Errorf("server %q already has %d of %d attachments: %w",
				serverInstanceNo, attached, limit, ErrAttachmentLimitReached)
		}
	}
	vol.ServerInstanceNo = serverInstanceNo
	vol.StatusCode = StatusAttached
	return nil
}

func (f *FakeCloud) DetachVolume(_ context.Context, volumeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	vol, ok := f.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}
	vol.ServerInstanceNo = ""
	vol.StatusCode = StatusCreated
	return nil
}

func (f *FakeCloud) EnsureDetachVolume(ctx context.Context, volumeID string) (*Volume, error) {
	if err := f.DetachVolume(ctx, volumeID); err != nil {
		return nil, err
	}
	return f.GetVolumeByID(ctx, volumeID)
}

func (f *FakeCloud) ResizeVolume(_ context.Context, volumeID string, newSizeGiB int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	vol, ok := f.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}
	newSize := int64(newSizeGiB) * utils.GiB
	if newSize < vol.SizeBytes {
		return fmt.Errorf("block storage %q cannot shrink from %d to %d bytes", volumeID, vol.SizeBytes, newSize)
	}
	vol.SizeBytes = newSize
	return nil
}

func (f *FakeCloud) WaitForVolumeResize(_ context.Context, volumeID string, minSizeBytes int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	vol, ok := f.volumes[volumeID]
	if !ok {
		return fmt.Errorf("volume %s disappeared while waiting for resize: %w", volumeID, ErrVolumeNotFound)
	}
	if vol.SizeBytes < minSizeBytes {
		return fmt.Errorf("volume %q has size %d, expected at least %d", volumeID, vol.SizeBytes, minSizeBytes)
	}
	return nil
}

func (f *FakeCloud) WaitForVolumeStatus(_ context.Context, volumeID string, desiredStatuses []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	vol, ok := f.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}
	for _, s := range desiredStatuses {
		if vol.StatusCode == s {
			return nil
		}
	}
	return fmt.Errorf("volume %q has status %q, expected one of %v", volumeID, vol.StatusCode, desiredStatuses)
}

func (f *FakeCloud) CreateSnapshot(_ context.Context, opts *SnapshotOptions) (*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(opts.Name) > maxResourceNameLength {
		return nil, fmt.Errorf("snapshot name %q exceeds %d characters", opts.Name, maxResourceNameLength)
	}
	for _, s := range f.snapshots {
		if s.Name == opts.Name {
			return nil, fmt.Errorf("snapshot name %q already in use: %w", opts.Name, ErrNameInUse)
		}
	}
	vol, ok := f.volumes[opts.SourceVolumeID]
	if !ok {
		return nil, fmt.Errorf("source volume %q: %w", opts.SourceVolumeID, ErrVolumeNotFound)
	}

	snap := &Snapshot{
		ID:             f.newID(),
		Name:           opts.Name,
		SourceVolumeID: vol.ID,
		SizeBytes:      vol.SizeBytes,
		StatusCode:     StatusCreated,
		OperationCode:  OpNormal,
		CreatedAt:      time.Now(),
		ReadyToUse:     true,
	}
	f.snapshots[snap.ID] = snap
	return copySnapshot(snap), nil
}

func (f *FakeCloud) DeleteSnapshot(_ context.Context, snapshotID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.snapshots[snapshotID]; !ok {
		return ErrSnapshotNotFound
	}
	delete(f.snapshots, snapshotID)
	return nil
}

func (f *FakeCloud) GetSnapshotByID(_ context.Context, snapshotID string) (*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snap, ok := f.snapshots[snapshotID]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	return copySnapshot(snap), nil
}

func (f *FakeCloud) GetSnapshotByName(_ context.Context, name string) (*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, snap := range f.snapshots {
		if snap.Name == name {
			return copySnapshot(snap), nil
		}
	}
	return nil, ErrSnapshotNotFound
}

func (f *FakeCloud) GetSnapshotsByVolumeID(_ context.Context, volumeID string) ([]*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []*Snapshot
	for _, snap := range f.snapshots {
		if snap.SourceVolumeID == volumeID {
			out = append(out, copySnapshot(snap))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *FakeCloud) ListSnapshots(_ context.Context) ([]*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]*Snapshot, 0, len(f.snapshots))
	for _, snap := range f.snapshots {
		out = append(out, copySnapshot(snap))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *FakeCloud) GetServerInstance(_ context.Context, serverInstanceNo string) (*ServerInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	inst, ok := f.servers[serverInstanceNo]
	if !ok {
		return nil, fmt.Errorf("get server instance %q: %w", serverInstanceNo, ErrServerInstanceNotFound)
	}
	c := *inst
	return &c, nil
}
