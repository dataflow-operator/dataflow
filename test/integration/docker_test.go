//go:build integration

package integration

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// skipUnlessDocker skips the test when Docker is unavailable for testcontainers.
func skipUnlessDocker(t *testing.T) {
	t.Helper()

	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("requires Docker: %v", err)
		return
	}

	var reason error
	func() {
		defer func() {
			if r := recover(); r != nil {
				reason = fmt.Errorf("%v", r)
			}
		}()
		_, reason = testcontainers.NewDockerClientWithOpts(context.Background())
	}()
	if reason != nil {
		t.Skipf("requires Docker: %v", reason)
	}
}

// requireDocker skips the test when err indicates Docker/testcontainers is unavailable.
func requireDocker(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if isDockerUnavailable(err) {
		t.Skipf("requires Docker: %v", err)
	}
	require.NoError(t, err)
}

func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "docker") ||
		strings.Contains(msg, "rootless") ||
		strings.Contains(msg, "cannot connect")
}
