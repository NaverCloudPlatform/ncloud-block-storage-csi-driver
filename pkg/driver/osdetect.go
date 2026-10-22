// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"bufio"
	"os"
	"strings"

	"k8s.io/klog/v2"
)

const (
	// defaultOsReleasePath is where the node DaemonSet mounts the host's
	// /etc/os-release file for OS family detection.
	defaultOsReleasePath = "/host/etc/os-release"

	fsTypeExt4 = "ext4"
	fsTypeXfs  = "xfs"
)

// rhelFamilyIDs lists os-release ID/ID_LIKE identifiers of distributions
// that use xfs as their default filesystem.
var rhelFamilyIDs = []string{
	"rhel",
	"centos",
	"rocky",
	"fedora",
	"alma",
	"navix",
}

// detectDefaultFsType parses the host's os-release file and returns the
// default filesystem type for the OS family: "xfs" for RHEL-family
// distributions (RHEL/CentOS/Rocky/Alma/NAVIX), "ext4" otherwise.
// If the file cannot be read it falls back to "ext4" for backward
// compatibility.
func detectDefaultFsType(osReleasePath string) string {
	file, err := os.Open(osReleasePath)
	if err != nil {
		klog.Warningf("failed to open %s, falling back to %s: %v", osReleasePath, fsTypeExt4, err)
		return fsTypeExt4
	}
	defer file.Close()

	var id, idLike string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "ID=") {
			id = stripQuotes(strings.TrimPrefix(line, "ID="))
		} else if strings.HasPrefix(line, "ID_LIKE=") {
			idLike = stripQuotes(strings.TrimPrefix(line, "ID_LIKE="))
		}
	}

	if err := scanner.Err(); err != nil {
		klog.Warningf("error reading %s, falling back to %s: %v", osReleasePath, fsTypeExt4, err)
		return fsTypeExt4
	}

	if isRHELFamily(id, idLike) {
		klog.V(2).Infof("detected RHEL-family OS (ID=%s, ID_LIKE=%s), default fsType: %s", id, idLike, fsTypeXfs)
		return fsTypeXfs
	}

	klog.V(2).Infof("detected OS (ID=%s, ID_LIKE=%s), default fsType: %s", id, idLike, fsTypeExt4)
	return fsTypeExt4
}

// isRHELFamily reports whether ID or ID_LIKE contains a RHEL-family identifier.
func isRHELFamily(id, idLike string) bool {
	combined := strings.ToLower(id + " " + idLike)
	for _, family := range rhelFamilyIDs {
		if strings.Contains(combined, family) {
			return true
		}
	}
	return false
}

// stripQuotes removes surrounding quotes from an os-release value.
func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[0] == s[len(s)-1] {
		return s[1 : len(s)-1]
	}
	return s
}
