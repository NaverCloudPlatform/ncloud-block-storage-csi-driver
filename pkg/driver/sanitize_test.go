// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"fmt"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
)

// TestStripSecretsFromRequestLogs guards against regressions of the request
// logging pattern: CSI sidecars inject credentials into req.Secrets, so any
// request logged without protosanitizer.StripSecrets would leak them at -v=4.
func TestStripSecretsFromRequestLogs(t *testing.T) {
	const (
		secretValue = "top-secret-access-key"
		volumeName  = "pvc-sanitize-test"
	)

	testCases := []struct {
		name string
		req  interface{ String() string }
	}{
		{
			name: "CreateVolumeRequest",
			req: &csi.CreateVolumeRequest{
				Name:    volumeName,
				Secrets: map[string]string{"accessKey": secretValue},
			},
		},
		{
			name: "ControllerPublishVolumeRequest",
			req: &csi.ControllerPublishVolumeRequest{
				VolumeId: volumeName,
				Secrets:  map[string]string{"secretKey": secretValue},
			},
		},
		{
			name: "NodeStageVolumeRequest",
			req: &csi.NodeStageVolumeRequest{
				VolumeId: volumeName,
				Secrets:  map[string]string{"secretKey": secretValue},
			},
		},
		{
			name: "CreateSnapshotRequest",
			req: &csi.CreateSnapshotRequest{
				Name:    volumeName,
				Secrets: map[string]string{"accessKey": secretValue},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logged := fmt.Sprintf("called with args %+v", protosanitizer.StripSecrets(tc.req))

			if strings.Contains(logged, secretValue) {
				t.Errorf("sanitized log output leaked secret value: %s", logged)
			}
			if !strings.Contains(logged, volumeName) {
				t.Errorf("sanitized log output should keep non-secret fields, got: %s", logged)
			}
		})
	}
}
