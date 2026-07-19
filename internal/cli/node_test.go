package cli

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
)

func TestNodeDrainDryRunReportsImpactAndKnownBlockers(t *testing.T) {
	t.Parallel()

	inventory := status.Result{
		Endpoint: "unix:///var/run/docker.sock",
		Cluster:  status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			{ID: "m3", Hostname: "manager-3", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active", Labels: map[string]string{"region": "east"}},
			{ID: "w2", Hostname: "worker-2", Role: "worker", State: "ready", Availability: "active", Labels: map[string]string{"region": "west"}},
			{ID: "w3", Hostname: "worker-3", Role: "worker", State: "ready", Availability: "active", Labels: map[string]string{"region": "east"}},
		},
		Services: []status.Service{
			{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 2, DesiredTasks: 2, Converged: true, PlacementConstraints: []string{"node.labels.region==east"}, MaxReplicasPerNode: 1},
			{ID: "s2", Name: "database", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Converged: true, StorageMounts: []status.StorageMount{{Type: "volume", Source: "database-data", Target: "/data", NodeLocal: true}}},
			{ID: "s3", Name: "agent", Mode: "global", RunningTasks: 6, DesiredTasks: 6, Converged: true},
		},
		DesiredTasks: []status.Task{
			{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t2", Name: "database.1", ServiceID: "s2", Service: "database", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t3", Name: "agent.worker-1", ServiceID: "s3", Service: "agent", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t4", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w3", Node: "worker-3", DesiredState: "running", State: "running"},
		},
		Tasks: []status.Task{
			{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t2", Name: "database.1", ServiceID: "s2", Service: "database", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t3", Name: "agent.worker-1", ServiceID: "s3", Service: "agent", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t4", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w3", Node: "worker-3", DesiredState: "running", State: "running"},
		},
	}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "drain", "worker-1", "--dry-run"}, &fakeConnector{connection: checkInspector{result: inventory}}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `DRAIN BLOCKED: worker-1
OFFLINE BLOCKED: worker-1
Target: worker-1 (worker, ready/active)

Drain blockers:
  service api has no eligible destination based on evaluated placement inputs
Manager offline checks: not applicable for worker targets

Replicated tasks expected to move:
  api.1 (api, running)
  database.1 (database, running)
Global tasks expected to stop:
  agent.worker-1 (agent, running)

Destination eligibility:
  api: no eligible destinations
    manager-1: constraint node.labels.region==east does not match
    manager-2: constraint node.labels.region==east does not match
    manager-3: constraint node.labels.region==east does not match
    worker-2: constraint node.labels.region==east does not match
    worker-3: service already has 1 active task, limit is 1
  database: manager-1, manager-2, manager-3, worker-2, worker-3

Storage portability warnings:
  database: volume database-data -> /data uses node-local storage
Unknown scheduler inputs: generic resources
Unevaluated placement constraints: none
`, stdout.String())
}

func TestNodeDrainDryRunJSONOutput(t *testing.T) {
	t.Parallel()

	inventory := status.Result{
		Endpoint: "unix:///var/run/docker.sock",
		Cluster:  status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", Role: "worker", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Converged: true},
			{ID: "s2", Name: "agent", Mode: "global", RunningTasks: 3, DesiredTasks: 3, Converged: true},
		},
		DesiredTasks: []status.Task{
			{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t2", Name: "agent.worker-1", ServiceID: "s2", Service: "agent", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		},
		Tasks: []status.Task{
			{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "t2", Name: "agent.worker-1", ServiceID: "s2", Service: "agent", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		},
	}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "drain", "worker-1", "--dry-run", "--json"}, &fakeConnector{connection: checkInspector{result: inventory}}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.JSONEq(t, `{
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
  "safe_to_drain": true,
  "safe_to_take_offline": true,
  "replicated_tasks": [
    {
      "id": "t1",
      "name": "api.1",
      "service_id": "s1",
      "service": "api",
      "state": "running"
    }
  ],
  "global_tasks": [
    {
      "id": "t2",
      "name": "agent.worker-1",
      "service_id": "s2",
      "service": "agent",
      "state": "running"
    }
  ],
  "unsupported_tasks": [],
  "service_impacts": [
    {
      "id": "s1",
      "name": "api",
      "task_count": 1,
      "estimated_task_capacity": 1,
      "eligible_destinations": [
        {"id": "m1", "hostname": "manager-1"},
        {"id": "w2", "hostname": "worker-2"}
      ],
      "blocked_destinations": [],
      "storage_portability_warnings": []
    }
  ],
  "unevaluated_inputs": ["generic_resources"],
  "unevaluated_constraints": [],
  "drain_findings": [
    {"gate": "target_exists", "level": "pass", "message": "target node worker-1 exists with role worker"},
    {"gate": "target_ready", "level": "pass", "message": "target node worker-1 is ready"},
    {"gate": "target_active", "level": "pass", "message": "target node worker-1 is active"},
    {"gate": "swarm_active", "level": "pass", "message": "connected Docker endpoint is part of an active Swarm"},
    {"gate": "swarm_control_available", "level": "pass", "message": "connected Docker endpoint provides Swarm manager control"},
    {"gate": "manager_healthy", "level": "pass", "message": "Swarm manager manager-1 is healthy (ready, active and leader)"},
    {"gate": "service_converged", "level": "pass", "message": "all 2 Swarm services are converged"},
    {"gate": "destination_eligible", "level": "pass", "message": "service api has 2 eligible destinations based on evaluated placement inputs"}
  ],
  "manager_offline_findings": []
}`, stdout.String())
}

func TestNodeDrainRejectsInvalidTimeoutBeforeConnecting(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	var stderr bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "drain", "worker-1", "--timeout", "0s"}, connector, &bytes.Buffer{}, &stderr)

	assert.Equal(t, ExitInvalidUsage, exitCode)
	assert.Empty(t, connector.contextName)
	assert.Equal(t, "usage: skepr [--context name] node drain <node> [--dry-run] [--timeout duration] [--json]\n", stderr.String())
}

func TestNodeDrainDrainsAndWaitsForConvergence(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	initial := healthyNodeDrainInventory()
	drained := healthyNodeDrainInventory()
	drained.Nodes[1].Availability = "drain"
	drained.DesiredTasks = nil
	drained.Tasks = nil
	connection := &nodeDrainConnection{inventories: []status.Result{initial, initial, drained, drained}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "drain", "worker-1", "--timeout", "1s"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, []nodeAvailabilityUpdate{{nodeID: "w1", availability: "drain"}}, connection.updates)
	assert.Equal(t, 4, connection.inspectCalls)
	assert.Equal(t, `DRAINED: worker-1
Target: worker-1 (w1)
Availability: drain
Replicated tasks moved: 1
Global tasks stopped: 0
Affected services converged: 1
`, stdout.String())
}

func TestNodeDrainJSONOutput(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	initial := healthyNodeDrainInventory()
	drained := healthyNodeDrainInventory()
	drained.Nodes[1].Availability = "drain"
	drained.DesiredTasks = nil
	drained.Tasks = nil
	connection := &nodeDrainConnection{inventories: []status.Result{initial, initial, drained, drained}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "drain", "worker-1", "--timeout", "1s", "--json"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.JSONEq(t, `{
  "schema_version": 1,
  "endpoint": "unix:///var/run/docker.sock",
  "cluster_id": "cluster-1",
  "target": {
    "id": "w1",
    "hostname": "worker-1",
    "role": "worker",
    "state": "ready",
    "availability": "drain"
  },
  "phase": "drained",
  "mutation_occurred": true,
  "availability": "drain",
  "replicated_tasks_moved": 1,
  "global_tasks_stopped": 0,
  "affected_services": [
    {"id": "s1", "name": "api", "mode": "replicated"}
  ],
  "evacuated": true,
  "services_converged": true
}`, stdout.String())
}

func TestNodeDrainUpdateFailureRequiresRecovery(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	initial := healthyNodeDrainInventory()
	connection := &nodeDrainConnection{inventories: []status.Result{initial, initial}, updateErr: fmt.Errorf("manager lost the update response")}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "drain", "worker-1", "--timeout", "1s"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitPartialMutation, exitCode)
	assert.Empty(t, stdout.String())
	assert.Equal(t, []nodeAvailabilityUpdate{{nodeID: "w1", availability: "drain"}}, connection.updates)
	assert.Contains(t, stderr.String(), "node drain failed: request drain for target node worker-1: manager lost the update response")
	assert.Contains(t, stderr.String(), "RECOVERY: node worker-1 may remain drained; inspect live state before activating it")
}

func TestNodeActivateActivatesAndVerifiesClusterHealth(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	drained := healthyNodeDrainInventory()
	drained.Nodes[1].Availability = "drain"
	drained.DesiredTasks = nil
	drained.Tasks = nil
	active := drained
	active.Nodes = append([]status.Node(nil), drained.Nodes...)
	active.Nodes[1].Availability = "active"
	connection := &nodeDrainConnection{inventories: []status.Result{drained, drained, active}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "activate", "worker-1", "--timeout", "1s"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, []nodeAvailabilityUpdate{{nodeID: "w1", availability: "active"}}, connection.updates)
	assert.Equal(t, 3, connection.inspectCalls)
	assert.Equal(t, `ACTIVE: worker-1
Target: worker-1 (w1)
Availability: active
Healthy managers: 1/1
Converged services: 1/1
`, stdout.String())
}

func TestNodeActivateJSONOutput(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	drained := healthyNodeDrainInventory()
	drained.Nodes[1].Availability = "drain"
	drained.DesiredTasks = nil
	drained.Tasks = nil
	active := drained
	active.Nodes = append([]status.Node(nil), drained.Nodes...)
	active.Nodes[1].Availability = "active"
	connection := &nodeDrainConnection{inventories: []status.Result{drained, drained, active}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "activate", "worker-1", "--timeout", "1s", "--json"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.JSONEq(t, `{
  "schema_version": 1,
  "endpoint": "unix:///var/run/docker.sock",
  "cluster_id": "cluster-1",
  "target": {
    "id": "w1",
    "hostname": "worker-1",
    "role": "worker",
    "state": "ready",
    "availability": "active"
  },
  "phase": "active",
  "mutation_occurred": true,
  "availability": "active",
  "healthy_managers": 1,
  "total_managers": 1,
  "converged_services": 1,
  "total_services": 1
}`, stdout.String())
}

func TestNodeActivateRejectsNodeThatIsNotDrained(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	connection := &nodeDrainConnection{inventories: []status.Result{healthyNodeDrainInventory()}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "activate", "worker-1", "--timeout", "1s"}, &fakeConnector{connection: connection}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Empty(t, connection.updates)
	assert.Equal(t, `ACTIVATION BLOCKED: worker-1
Target: worker-1 (worker, ready/active)

Blockers:
  target node worker-1 availability is active, expected drain
`, stdout.String())
}

func TestNodeActivateUpdateFailureRequiresRecoveryWithoutRedraining(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	drained := healthyNodeDrainInventory()
	drained.Nodes[1].Availability = "drain"
	drained.DesiredTasks = nil
	drained.Tasks = nil
	connection := &nodeDrainConnection{inventories: []status.Result{drained, drained}, updateErr: fmt.Errorf("manager lost the update response")}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run(context.Background(), []string{"node", "activate", "worker-1", "--timeout", "1s"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitPartialMutation, exitCode)
	assert.Empty(t, stdout.String())
	assert.Equal(t, []nodeAvailabilityUpdate{{nodeID: "w1", availability: "active"}}, connection.updates)
	assert.Contains(t, stderr.String(), "node activation failed: request activation for target node worker-1: manager lost the update response")
	assert.Contains(t, stderr.String(), "RECOVERY: node worker-1 may be active; inspect live state before making another availability change")
}

type nodeDrainConnection struct {
	inventories  []status.Result
	inspectCalls int
	updates      []nodeAvailabilityUpdate
	updateErr    error
}

type nodeAvailabilityUpdate struct {
	nodeID       string
	availability string
}

func (c *nodeDrainConnection) Inspect(context.Context) (status.Result, error) {
	if c.inspectCalls >= len(c.inventories) {
		return status.Result{}, fmt.Errorf("unexpected inspect call %d", c.inspectCalls+1)
	}
	result := c.inventories[c.inspectCalls]
	c.inspectCalls++
	return result, nil
}

func (c *nodeDrainConnection) UpdateNodeAvailability(_ context.Context, nodeID, availability string) error {
	c.updates = append(c.updates, nodeAvailabilityUpdate{nodeID: nodeID, availability: availability})
	return c.updateErr
}

func (c *nodeDrainConnection) Close() error { return nil }

func healthyNodeDrainInventory() status.Result {
	task := status.Task{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"}
	return status.Result{
		Endpoint: "unix:///var/run/docker.sock",
		Cluster:  status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", Role: "worker", State: "ready", Availability: "active"},
		},
		Services:     []status.Service{{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Converged: true}},
		DesiredTasks: []status.Task{task},
		Tasks:        []status.Task{task},
	}
}
