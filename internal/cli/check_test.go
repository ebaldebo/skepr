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
	exitCode := Run(context.Background(), []string{"check", "worker-1"}, &fakeConnector{connection: healthyCheckConnection()}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, `PASS: target node worker-1 exists with role worker
PASS: target node worker-1 is ready
PASS: target node worker-1 is active
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
PASS: Swarm manager manager-1 is healthy (ready, active and leader)
PASS: Swarm manager manager-2 is healthy (ready, active and reachable)
PASS: all 3 Swarm services are converged
SAFE: target node worker-1 passed checks

Target workloads: 0 desired-running tasks across 0 affected services
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

Target workloads: 0 desired-running tasks across 0 affected services
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

Target workloads: 0 desired-running tasks across 0 affected services
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

Target workloads: 0 desired-running tasks across 0 affected services
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
			{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			{ID: "m3", Hostname: "manager-3", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "manager-1"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `BLOCKER: target manager manager-1 is the current Swarm leader
PASS: target node manager-1 exists with role manager
PASS: target node manager-1 is ready
PASS: target node manager-1 is active
PASS: taking target manager manager-1 offline leaves 2 healthy managers; quorum requires 2 of 3
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
PASS: Swarm manager manager-1 is healthy (ready, active and leader)
PASS: Swarm manager manager-2 is healthy (ready, active and reachable)
PASS: Swarm manager manager-3 is healthy (ready, active and reachable)
UNSAFE: target node manager-1 failed checks

Target workloads: 0 desired-running tasks across 0 affected services
`, stdout.String())
}

func TestCheckBlocksUnsafeManagerQuorum(t *testing.T) {
	t.Parallel()

	connection := checkInspector{result: status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "manager-2"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `BLOCKER: taking target manager manager-2 offline leaves 1 healthy manager; quorum requires 2 of 2
PASS: target node manager-2 exists with role manager
PASS: target node manager-2 is ready
PASS: target node manager-2 is active
PASS: target manager manager-2 is not the current Swarm leader
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
PASS: Swarm manager manager-1 is healthy (ready, active and leader)
PASS: Swarm manager manager-2 is healthy (ready, active and reachable)
UNSAFE: target node manager-2 failed checks

Target workloads: 0 desired-running tasks across 0 affected services
`, stdout.String())
}

func TestCheckBlocksUnconvergedService(t *testing.T) {
	t.Parallel()

	connection := checkInspector{result: status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "s1", Name: "api", RunningTasks: 2, DesiredTasks: 2, Converged: true},
			{ID: "s2", Name: "database", RunningTasks: 0, DesiredTasks: 1, Converged: false},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `BLOCKER: Swarm service database has 0 of 1 running tasks
PASS: target node worker-1 exists with role worker
PASS: target node worker-1 is ready
PASS: target node worker-1 is active
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
UNSAFE: target node worker-1 failed checks

Target workloads: 0 desired-running tasks across 0 affected services
`, stdout.String())
}

func TestCheckPrintsTargetWorkloadInventory(t *testing.T) {
	t.Parallel()

	connection := checkInspector{result: status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		},
		DesiredTasks: []status.Task{
			{ID: "t2", Name: "database.1", ServiceID: "s2", Service: "database", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "rejected"},
			{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		},
	}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, `PASS: target node worker-1 exists with role worker
PASS: target node worker-1 is ready
PASS: target node worker-1 is active
PASS: connected Docker endpoint is part of an active Swarm
PASS: connected Docker endpoint provides Swarm manager control
SAFE: target node worker-1 passed checks

Target workloads: 2 desired-running tasks across 2 affected services
  TASK NAME   TASK ID  SERVICE   SERVICE ID  STATE
  api.1       t1       api       s1          running
  database.1  t2       database  s2          rejected
`, stdout.String())
}

func TestCheckJSONOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"check", "worker-1", "--json"}, &fakeConnector{connection: workloadCheckConnection()}, &stdout, &bytes.Buffer{})

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
  "target_workload": {
    "desired_running_task_count": 2,
    "tasks": [
      {
        "id": "t1",
        "name": "api.1",
        "service_id": "s1",
        "service": "api",
        "state": "running"
      },
      {
        "id": "t2",
        "name": "database.1",
        "service_id": "s2",
        "service": "database",
        "state": "starting"
      }
    ],
    "affected_services": [
      {
        "id": "s1",
        "name": "api"
      },
      {
        "id": "s2",
        "name": "database"
      }
    ]
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
    },
    {
      "gate": "service_converged",
      "level": "pass",
      "message": "all 3 Swarm services are converged"
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

func healthyCheckConnection() status.Connection {
	return checkInspector{result: healthyCheckResult()}
}

func workloadCheckConnection() status.Connection {
	result := healthyCheckResult()
	result.DesiredTasks = []status.Task{
		{ID: "t2", Name: "database.1", ServiceID: "s2", Service: "database", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "starting"},
		{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
	}
	return checkInspector{result: result}
}

func healthyCheckResult() status.Result {
	result, _ := fakeInspector{}.Inspect(context.Background())
	for i := range result.Services {
		result.Services[i].RunningTasks = result.Services[i].DesiredTasks
		result.Services[i].Converged = true
	}
	result.UnhealthyTasks = nil
	return result
}
