//go:build integration

package swarm_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"os"
	"testing"

	"github.com/ebaldebo/skepr/internal/cli"
	skeprdocker "github.com/ebaldebo/skepr/internal/docker"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthyFiveNodeSwarm(t *testing.T) {
	endpoint := os.Getenv("SKEPR_INTEGRATION_DOCKER_HOST")
	require.NotEmpty(t, endpoint, "run integration/swarm/run to create the disposable Swarm")
	parsedEndpoint, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.Equal(t, "tcp", parsedEndpoint.Scheme)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, parsedEndpoint.Hostname())

	t.Setenv("DOCKER_HOST", endpoint)
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	connector := skeprdocker.NewConnector()
	var statusOutput bytes.Buffer
	var statusErrors bytes.Buffer
	exitCode := cli.Run(context.Background(), []string{"status", "--json"}, connector, &statusOutput, &statusErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, statusErrors.String())

	var inventory status.Result
	require.NoError(t, json.Unmarshal(statusOutput.Bytes(), &inventory))
	assert.Len(t, inventory.Nodes, 5)
	managerCount := 0
	workerCount := 0
	for _, node := range inventory.Nodes {
		switch node.Role {
		case "manager":
			managerCount++
		case "worker":
			workerCount++
		}
	}
	assert.Equal(t, 3, managerCount)
	assert.Equal(t, 2, workerCount)

	var checkOutput bytes.Buffer
	var checkErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"check", "worker-1"}, connector, &checkOutput, &checkErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, checkErrors.String())
	assert.Contains(t, checkOutput.String(), "PASS: Swarm manager manager-1 is healthy")
	assert.Contains(t, checkOutput.String(), "PASS: Swarm manager manager-2 is healthy")
	assert.Contains(t, checkOutput.String(), "PASS: Swarm manager manager-3 is healthy")
	assert.Contains(t, checkOutput.String(), "SAFE: target node worker-1 passed checks")
	assert.Contains(t, checkOutput.String(), "Target workloads: 0 desired-running tasks across 0 affected services")

	var beginOutput bytes.Buffer
	var beginErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "begin", "worker-1", "--timeout", "30s"}, connector, &beginOutput, &beginErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, beginErrors.String())
	assert.Contains(t, beginOutput.String(), "Target: worker-1 (")
	assert.Contains(t, beginOutput.String(), "Phase: maintenance-ready")

	connection, err := connector.Connect(context.Background(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	inventory, err = connection.Inspect(context.Background())
	require.NoError(t, err)
	workerAvailability := ""
	for _, node := range inventory.Nodes {
		if node.Hostname == "worker-1" {
			workerAvailability = node.Availability
		}
	}
	assert.Equal(t, "drain", workerAvailability)
}
