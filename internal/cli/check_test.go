package cli

import (
	"bytes"
	"context"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
)

func TestCheckPrintsPassingNodeGates(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "worker-1"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, `PASS: target node worker-1 exists with role worker
PASS: target node worker-1 is ready
PASS: target node worker-1 is active
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
SAFE: target node worker-1 passed checks
`, stdout.String())
}

func TestCheckPrintsBlockersBeforePasses(t *testing.T) {
	t.Parallel()

	connection := checkInspector{result: status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "down", Availability: "active"},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "w1"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `BLOCKER: target node worker-1 state is down, expected ready
PASS: target node worker-1 exists with role worker
PASS: target node worker-1 is active
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
UNSAFE: target node worker-1 failed checks
`, stdout.String())
}

func TestCheckBlocksEndpointWithoutActiveSwarmControl(t *testing.T) {
	t.Parallel()

	connection := checkInspector{result: status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "inactive", ControlAvailable: false},
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `BLOCKER: connected Docker endpoint Swarm state is inactive, expected active
BLOCKER: connected Docker endpoint does not provide Swarm manager control
PASS: target node worker-1 exists with role worker
PASS: target node worker-1 is ready
PASS: target node worker-1 is active
UNSAFE: target node worker-1 failed checks
`, stdout.String())
}

func TestCheckJSONOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "worker-1", "--json"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, `{
  "schema_version": 1,
  "endpoint": "unix:///var/run/docker.sock",
  "requested_node": "worker-1",
  "target": {
    "id": "w1",
    "hostname": "worker-1",
    "role": "worker",
    "state": "ready",
    "availability": "active"
  },
  "safe": true,
  "findings": [
    {
      "gate": "target_exists",
      "level": "pass",
      "message": "target node worker-1 exists with role worker"
    },
    {
      "gate": "target_ready",
      "level": "pass",
      "message": "target node worker-1 is ready"
    },
    {
      "gate": "target_active",
      "level": "pass",
      "message": "target node worker-1 is active"
    },
    {
      "gate": "swarm_active",
      "level": "pass",
      "message": "connected Docker endpoint is part of an active Swarm"
    },
    {
      "gate": "swarm_control_available",
      "level": "pass",
      "message": "connected Docker endpoint provides Swarm manager control"
    }
  ]
}
`, stdout.String())
}

type checkInspector struct {
	result status.Result
}

func (c checkInspector) Inspect(context.Context) (status.Result, error) {
	return c.result, nil
}

func (checkInspector) Close() error { return nil }
