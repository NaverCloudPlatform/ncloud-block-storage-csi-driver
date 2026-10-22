// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package mount

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMkfsArgs(t *testing.T) {
	preset := filepath.Join(t.TempDir(), "lts_5.15.conf")
	if err := os.WriteFile(preset, []byte("[metadata]\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("ext4 forces overwrite", func(t *testing.T) {
		args, err := mkfsArgs("ext4", "/dev/vdb", preset)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Join(args, " ") != "-F /dev/vdb" {
			t.Errorf("unexpected args: %v", args)
		}
	})

	t.Run("xfs pins the feature preset and forces overwrite", func(t *testing.T) {
		args, err := mkfsArgs("xfs", "/dev/vdb", preset)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := "-c options=" + preset + " -f /dev/vdb"
		if strings.Join(args, " ") != want {
			t.Errorf("expected %q, got %q", want, strings.Join(args, " "))
		}
	})

	t.Run("missing xfs preset is a packaging error, not a silent fallback", func(t *testing.T) {
		if _, err := mkfsArgs("xfs", "/dev/vdb", filepath.Join(t.TempDir(), "missing.conf")); err == nil {
			t.Fatal("expected error for missing preset, got nil")
		}
	})

	t.Run("other filesystems pass only the source", func(t *testing.T) {
		args, err := mkfsArgs("btrfs", "/dev/vdb", preset)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Join(args, " ") != "/dev/vdb" {
			t.Errorf("unexpected args: %v", args)
		}
	})
}

// findmnt exiting 1 with no output means "nothing mounted there"; any
// other failure must propagate instead of being read as "not mounted"
// (callers skip unmount on false, and kubelet would detach a still-mounted
// disk).
func TestIsMountedRefusesToGuessOnFindmntFailure(t *testing.T) {
	dir := t.TempDir()
	shim := filepath.Join(dir, "findmnt")
	write := func(script string) {
		t.Helper()
		if err := os.WriteFile(shim, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	m := NewSafeMounter()

	write("#!/bin/sh\nexit 1\n")
	mounted, err := m.IsMounted("/nowhere")
	if err != nil || mounted {
		t.Fatalf("exit 1 with no output should mean not mounted, got mounted=%v err=%v", mounted, err)
	}

	write("#!/bin/sh\nexit 7\n")
	if _, err := m.IsMounted("/nowhere"); err == nil {
		t.Fatal("expected an error for a non-1 findmnt exit code, got none")
	}

	write("#!/bin/sh\necho boom >&2\nexit 1\n")
	if _, err := m.IsMounted("/nowhere"); err == nil {
		t.Fatal("expected an error for exit 1 with output, got none")
	}
}

// findmnt prints the canonical mountinfo path; the request may arrive
// through symlinks (e.g. a symlinked kubelet root-dir) or with a trailing
// slash. IsMounted must canonicalize before comparing — reading a mounted
// path as "not mounted" makes NodeUnstage skip the unmount and kubelet
// detach a still-mounted disk.
func TestIsMountedCanonicalizesTheTarget(t *testing.T) {
	real := t.TempDir()
	canonReal, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "kubelet-link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}

	shimDir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s' '{\"filesystems\":[{\"target\":\"" + canonReal + "\",\"propagation\":\"shared\",\"fstype\":\"ext4\",\"options\":\"rw\"}]}'\n"
	if err := os.WriteFile(filepath.Join(shimDir, "findmnt"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shimDir)
	m := NewSafeMounter()

	for _, target := range []string{canonReal, link, canonReal + "/"} {
		mounted, err := m.IsMounted(target)
		if err != nil {
			t.Fatalf("IsMounted(%q): unexpected error: %v", target, err)
		}
		if !mounted {
			t.Errorf("IsMounted(%q) = false; the canonical mountpoint %q is mounted", target, canonReal)
		}
	}
}
