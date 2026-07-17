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
PASS: Swarm manager manager-1 is healthy (ready, active and leader)
PASS: Swarm manager manager-2 is healthy (ready, active and reachable)
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

func TestCheckBlocksUnhealthySwarmManager(t *testing.T) {
	t.Parallel()

	connection := checkInspector{result: status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "m2", Hostname: "manager-2", Role: "manager", State: "down", Availability: "drain", ManagerStatus: "unreachable"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `BLOCKER: Swarm manager manager-2 is unhealthy: state is down, expected ready; availability is drain, expected active; manager status is unreachable, expected leader or reachable
PASS: target node worker-1 exists with role worker
PASS: target node worker-1 is ready
PASS: target node worker-1 is active
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
PASS: Swarm manager manager-1 is healthy (ready, active and leader)
UNSAFE: target node worker-1 failed checks
`, stdout.String())
}

func TestCheckBlocksLeaderTarget(t *testing.T) {
	t.Parallel()

	connection := checkInspector{result: status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "manager-1"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `BLOCKER: target manager manager-1 is the current Swarm leader
PASS: target node manager-1 exists with role manager
PASS: target node manager-1 is ready
PASS: target node manager-1 is active
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
PASS: Swarm manager manager-1 is healthy (ready, active and leader)
UNSAFE: target node manager-1 failed checks
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
    },
    {
      "gate": "manager_healthy",
      "level": "pass",
      "message": "Swarm manager manager-1 is healthy (ready, active and leader)"
    },
    {
      "gate": "manager_healthy",
      "level": "pass",
      "message": "Swarm manager manager-2 is healthy (ready, active and reachable)"
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
