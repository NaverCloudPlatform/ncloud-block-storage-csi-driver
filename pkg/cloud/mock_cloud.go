// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package cloud

import "context"

type MockCloud struct {
	CreateVolumeFn           func(ctx context.Context, opts *VolumeOptions) (*Volume, error)
	DeleteVolumeFn           func(ctx context.Context, volumeID string) error
	GetVolumeByIDFn          func(ctx context.Context, volumeID string) (*Volume, error)
	GetVolumeByNameFn        func(ctx context.Context, name string) (*Volume, error)
	ListVolumesFn            func(ctx context.Context) ([]*Volume, error)
	AttachVolumeFn           func(ctx context.Context, volumeID, serverInstanceNo string) error
	DetachVolumeFn           func(ctx context.Context, volumeID string) error
	EnsureDetachVolumeFn     func(ctx context.Context, volumeID string) (*Volume, error)
	ResizeVolumeFn           func(ctx context.Context, volumeID string, newSizeGiB int32) error
	WaitForVolumeResizeFn    func(ctx context.Context, volumeID string, minSizeBytes int64) error
	WaitForVolumeStatusFn    func(ctx context.Context, volumeID string, desiredStatuses []string) error
	CreateSnapshotFn         func(ctx context.Context, opts *SnapshotOptions) (*Snapshot, error)
	DeleteSnapshotFn         func(ctx context.Context, snapshotID string) error
	GetSnapshotByIDFn        func(ctx context.Context, snapshotID string) (*Snapshot, error)
	GetSnapshotByNameFn      func(ctx context.Context, name string) (*Snapshot, error)
	GetSnapshotsByVolumeIDFn func(ctx context.Context, volumeID string) ([]*Snapshot, error)
	ListSnapshotsFn          func(ctx context.Context) ([]*Snapshot, error)
	GetServerInstanceFn      func(ctx context.Context, serverInstanceNo string) (*ServerInstance, error)
}

var _ Cloud = &MockCloud{}

func (m *MockCloud) CreateVolume(ctx context.Context, opts *VolumeOptions) (*Volume, error) {
	return m.CreateVolumeFn(ctx, opts)
}
func (m *MockCloud) DeleteVolume(ctx context.Context, volumeID string) error {
	return m.DeleteVolumeFn(ctx, volumeID)
}
func (m *MockCloud) GetVolumeByID(ctx context.Context, volumeID string) (*Volume, error) {
	return m.GetVolumeByIDFn(ctx, volumeID)
}
func (m *MockCloud) GetVolumeByName(ctx context.Context, name string) (*Volume, error) {
	return m.GetVolumeByNameFn(ctx, name)
}
func (m *MockCloud) ListVolumes(ctx context.Context) ([]*Volume, error) {
	return m.ListVolumesFn(ctx)
}
func (m *MockCloud) AttachVolume(ctx context.Context, volumeID, serverInstanceNo string) error {
	return m.AttachVolumeFn(ctx, volumeID, serverInstanceNo)
}
func (m *MockCloud) DetachVolume(ctx context.Context, volumeID string) error {
	return m.DetachVolumeFn(ctx, volumeID)
}
func (m *MockCloud) EnsureDetachVolume(ctx context.Context, volumeID string) (*Volume, error) {
	return m.EnsureDetachVolumeFn(ctx, volumeID)
}
func (m *MockCloud) ResizeVolume(ctx context.Context, volumeID string, newSizeGiB int32) error {
	return m.ResizeVolumeFn(ctx, volumeID, newSizeGiB)
}
func (m *MockCloud) WaitForVolumeResize(ctx context.Context, volumeID string, minSizeBytes int64) error {
	return m.WaitForVolumeResizeFn(ctx, volumeID, minSizeBytes)
}
func (m *MockCloud) WaitForVolumeStatus(ctx context.Context, volumeID string, desiredStatuses []string) error {
	return m.WaitForVolumeStatusFn(ctx, volumeID, desiredStatuses)
}
func (m *MockCloud) CreateSnapshot(ctx context.Context, opts *SnapshotOptions) (*Snapshot, error) {
	return m.CreateSnapshotFn(ctx, opts)
}
func (m *MockCloud) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return m.DeleteSnapshotFn(ctx, snapshotID)
}
func (m *MockCloud) GetSnapshotByID(ctx context.Context, snapshotID string) (*Snapshot, error) {
	return m.GetSnapshotByIDFn(ctx, snapshotID)
}
func (m *MockCloud) GetSnapshotByName(ctx context.Context, name string) (*Snapshot, error) {
	return m.GetSnapshotByNameFn(ctx, name)
}
func (m *MockCloud) GetSnapshotsByVolumeID(ctx context.Context, volumeID string) ([]*Snapshot, error) {
	return m.GetSnapshotsByVolumeIDFn(ctx, volumeID)
}
func (m *MockCloud) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	return m.ListSnapshotsFn(ctx)
}
func (m *MockCloud) GetServerInstance(ctx context.Context, serverInstanceNo string) (*ServerInstance, error) {
	return m.GetServerInstanceFn(ctx, serverInstanceNo)
}
