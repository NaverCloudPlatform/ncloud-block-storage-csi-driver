// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"os"
	"path/filepath"
	"testing"
)

func writeOsRelease(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "os-release")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write os-release fixture: %v", err)
	}
	return path
}

func TestDetectDefaultFsType(t *testing.T) {
	testCases := []struct {
		name     string
		content  string
		expected string
	}{
		{
			name:     "ubuntu returns ext4",
			content:  "NAME=\"Ubuntu\"\nID=ubuntu\nID_LIKE=debian\n",
			expected: fsTypeExt4,
		},
		{
			name:     "rocky returns xfs",
			content:  "NAME=\"Rocky Linux\"\nID=\"rocky\"\nID_LIKE=\"rhel centos fedora\"\n",
			expected: fsTypeXfs,
		},
		{
			name:     "navix returns xfs",
			content:  "NAME=\"NAVIX\"\nID='navix'\nID_LIKE='rhel'\n",
			expected: fsTypeXfs,
		},
		{
			name:     "id_like only match returns xfs",
			content:  "ID=someos\nID_LIKE=\"centos rhel\"\n",
			expected: fsTypeXfs,
		},
		{
			name:     "unknown os returns ext4",
			content:  "ID=alpine\n",
			expected: fsTypeExt4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeOsRelease(t, tc.content)
			if got := detectDefaultFsType(path); got != tc.expected {
				t.Errorf("detectDefaultFsType() = %q, want %q", got, tc.expected)
			}
		})
	}

	t.Run("missing file falls back to ext4", func(t *testing.T) {
		if got := detectDefaultFsType(filepath.Join(t.TempDir(), "does-not-exist")); got != fsTypeExt4 {
			t.Errorf("detectDefaultFsType() = %q, want %q", got, fsTypeExt4)
		}
	})
}
