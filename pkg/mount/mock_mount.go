// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package mount

type MockMounter struct {
	FormatFn                 func(source, fsType string) error
	MountFn                  func(source, target, fsType string, options ...string) error
	UnmountFn                func(target string) error
	IsFormattedFn            func(source string) (bool, error)
	IsMountedFn              func(target string) (bool, error)
	GetDevicePathFromMountFn func(mountPath string) (string, error)
	GetMountOptionsFn        func(mountPath string) ([]string, error)
	GetFsTypeFn              func(devicePath string) (string, error)
	ResizeFsFn               func(volumePath string) error
	GetStatisticsFn          func(volumePath string) (VolumeStatistics, error)
	IsBlockDeviceFn          func(volumePath string) (bool, error)
	SameDeviceFn             func(a, b string) (bool, error)
}

var _ Mounter = &MockMounter{}

func (m *MockMounter) Format(source, fsType string) error {
	return m.FormatFn(source, fsType)
}

func (m *MockMounter) Mount(source, target, fsType string, options ...string) error {
	return m.MountFn(source, target, fsType, options...)
}

func (m *MockMounter) Unmount(target string) error {
	return m.UnmountFn(target)
}

func (m *MockMounter) IsFormatted(source string) (bool, error) {
	return m.IsFormattedFn(source)
}

func (m *MockMounter) IsMounted(target string) (bool, error) {
	return m.IsMountedFn(target)
}

func (m *MockMounter) GetDevicePathFromMount(mountPath string) (string, error) {
	return m.GetDevicePathFromMountFn(mountPath)
}

func (m *MockMounter) GetMountOptions(mountPath string) ([]string, error) {
	return m.GetMountOptionsFn(mountPath)
}

func (m *MockMounter) GetFsType(devicePath string) (string, error) {
	return m.GetFsTypeFn(devicePath)
}

func (m *MockMounter) ResizeFs(volumePath string) error {
	return m.ResizeFsFn(volumePath)
}

func (m *MockMounter) GetStatistics(volumePath string) (VolumeStatistics, error) {
	return m.GetStatisticsFn(volumePath)
}

func (m *MockMounter) IsBlockDevice(volumePath string) (bool, error) {
	return m.IsBlockDeviceFn(volumePath)
}

func (m *MockMounter) SameDevice(a, b string) (bool, error) {
	return m.SameDeviceFn(a, b)
}
