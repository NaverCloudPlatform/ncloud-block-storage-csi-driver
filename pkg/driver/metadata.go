// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

const (
	metadataEndpointDefault = "http://169.254.169.254"
	metadataBasePath        = "/latest/meta-data/"
	metadataTokenPath       = "/latest/api/token"
	metadataTokenTTL        = "21600"
	metadataClientTimeout   = 3 * time.Second
)

// metadataClient retrieves instance metadata using Ncloud IMDSv2 (token-based).
// The token-based flow prevents SSRF attacks by requiring a PUT with a
// TTL header before issuing GET requests. The token is acquired once and
// reused; when the token endpoint fails, the values are requested without a
// token, which the metadata service also serves (checked on a live node:
// token-less GET 200, wrong token 401), so the driver still starts.
type metadataClient struct {
	endpoint string
	client   *http.Client
	token    string
}

func newMetadataClient() *metadataClient {
	endpoint := metadataEndpointDefault
	if v := os.Getenv("NCLOUD_METADATA_API_ENDPOINT"); v != "" {
		endpoint = v
	}
	return &metadataClient{
		endpoint: endpoint,
		client:   &http.Client{Timeout: metadataClientTimeout},
	}
}

// acquireToken performs the IMDSv2 PUT request to obtain a session token.
func (c *metadataClient) acquireToken() (string, error) {
	req, err := http.NewRequest(http.MethodPut, c.endpoint+metadataTokenPath, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-NCP-METADATA-TOKEN-TTL-SECONDS", metadataTokenTTL)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata token request failed with status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

// get retrieves a single metadata value by path (e.g. "serverInstanceNo").
// The session token is acquired on the first call and reused; if it cannot
// be acquired the value is requested without one, and a token the service
// no longer accepts is renewed once.
func (c *metadataClient) get(path string) (string, error) {
	if c.token == "" {
		token, err := c.acquireToken()
		if err != nil {
			klog.Warningf("metadata token request failed (%v); requesting %q without a token", err, path)
		} else {
			c.token = token
		}
	}

	value, statusCode, err := c.doGet(path, c.token)
	if err == nil && statusCode == http.StatusUnauthorized && c.token != "" {
		c.token = ""
		token, terr := c.acquireToken()
		if terr != nil {
			return "", fmt.Errorf("metadata request for %q was refused and the token could not be renewed: %w", path, terr)
		}
		c.token = token
		value, statusCode, err = c.doGet(path, c.token)
	}
	if err != nil {
		return "", err
	}
	if statusCode != http.StatusOK {
		return "", fmt.Errorf("metadata request for %q failed with status %d", path, statusCode)
	}
	return value, nil
}

// doGet performs one metadata GET, with the token header when token is set.
// IMDS terminates every value with a newline ("KR-2\n"); the value is
// trimmed because it is used verbatim -- the zone code as the zone-table
// key, the instance number as the node ID and as an API parameter.
func (c *metadataClient) doGet(path, token string) (string, int, error) {
	req, err := http.NewRequest(http.MethodGet, c.endpoint+metadataBasePath+path, nil)
	if err != nil {
		return "", 0, err
	}
	if token != "" {
		req.Header.Set("X-NCP-METADATA-TOKEN", token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", resp.StatusCode, err
	}
	return strings.TrimSpace(string(body)), resp.StatusCode, nil
}
