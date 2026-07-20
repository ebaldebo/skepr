package cli

import (
	"bytes"
	"context"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
)

func TestRebalanceReportShowsKnownRedistributionOpportunities(t *testing.T) {
	t.Parallel()

	inventory := rebalanceTestInventory()
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"rebalance", "report"}, &fakeConnector{connection: rebalanceInspector{result: inventory}}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, `REBALANCE OPPORTUNITIES: 1
Assessed replicated services: 1
Not assessed services: 1

Opportunities:
  api: 4 replicas, skew 4
    distribution: worker-1=4, worker-2=0, worker-3=0
    overloaded nodes: worker-1
    known eligible destinations: worker-2, worker-3
    unevaluated inputs: generic resources

Not assessed:
  agent: mode global is not replicated
`, stdout.String())
}

func TestRebalanceReportJSONOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"rebalance", "report", "--json"}, &fakeConnector{connection: rebalanceInspector{result: rebalanceTestInventory()}}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.JSONEq(t, `{
  "schema_version": 1,
  "endpoint": "unix:///var/run/docker.sock",
  "cluster_id": "cluster-1",
  "summary": {
    "replicated_services": 1,
    "assessed_services": 1,
    "opportunities": 1,
    "constrained_services": 0,
    "not_assessed_services": 1
  },
  "services": [
    {
      "id": "s2",
      "name": "agent",
      "mode": "global",
      "replicas": 3,
      "state": "not-assessed",
      "reason": "mode global is not replicated",
      "skew": 0,
      "distribution": [],
      "overloaded_nodes": [],
      "known_eligible_destinations": [],
      "storage_warnings": [],
      "unevaluated_inputs": [],
      "unevaluated_constraints": []
    },
    {
      "id": "s1",
      "name": "api",
      "mode": "replicated",
      "replicas": 4,
      "state": "opportunity",
      "skew": 4,
      "distribution": [
        {"id": "w1", "hostname": "worker-1", "tasks": 4},
        {"id": "w2", "hostname": "worker-2", "tasks": 0},
        {"id": "w3", "hostname": "worker-3", "tasks": 0}
      ],
      "overloaded_nodes": [
        {"id": "w1", "hostname": "worker-1", "tasks": 4}
      ],
      "known_eligible_destinations": [
        {"id": "w2", "hostname": "worker-2", "tasks": 0},
        {"id": "w3", "hostname": "worker-3", "tasks": 0}
      ],
      "storage_warnings": [],
      "unevaluated_inputs": ["generic_resources"],
      "unevaluated_constraints": []
    }
  ]
}`, stdout.String())
}

func TestRebalanceReportCheckReturnsNonzeroForOpportunities(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"rebalance", "report", "--check"}, &fakeConnector{connection: rebalanceInspector{result: rebalanceTestInventory()}}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Contains(t, stdout.String(), "REBALANCE OPPORTUNITIES: 1")
}

func TestRebalanceReportRequiresActiveSwarmManagerControl(t *testing.T) {
	t.Parallel()

	inventory := rebalanceTestInventory()
	inventory.Cluster.ControlAvailable = false
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := Run(context.Background(), []string{"rebalance", "report"}, &fakeConnector{connection: rebalanceInspector{result: inventory}}, &stdout, &stderr)

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Empty(t, stdout.String())
	assert.Equal(t, "rebalance report requires active Swarm manager control\n", stderr.String())
}

type rebalanceInspector struct {
	result status.Result
}

func (i rebalanceInspector) Inspect(context.Context) (status.Result, error) {
	return i.result, nil
}

func (rebalanceInspector) Close() error { return nil }

func rebalanceTestInventory() status.Result {
	tasks := []status.Task{
		{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t2", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t3", Name: "api.3", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t4", Name: "api.4", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
	}
	return status.Result{
		Endpoint: "unix:///var/run/docker.sock",
		Cluster:  status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", Role: "worker", State: "ready", Availability: "active"},
			{ID: "w3", Hostname: "worker-3", Role: "worker", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 4, DesiredTasks: 4, Converged: true},
			{ID: "s2", Name: "agent", Mode: "global", RunningTasks: 3, DesiredTasks: 3, Converged: true},
		},
		DesiredTasks: tasks,
		Tasks:        tasks,
	}
}
