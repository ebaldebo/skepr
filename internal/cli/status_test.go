package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
)

func TestStatusPrintsClusterSummary(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"status"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `DEGRADED
  task database.1 is rejected: no suitable node
  service database has 0/1 running tasks

Managers: 2/2 healthy
Nodes: 3/3 ready, 3 active
Services: 2/3 converged
Cluster: cluster-1
Endpoint: unix:///var/run/docker.sock
Swarm: active
Control: available
Leader: manager-1

Nodes:
  manager-1  m1  manager  ready  active  leader
  manager-2  m2  manager  ready  active  reachable
  worker-1   w1  worker   ready  active

Services:
  database  s2  replicated  0/1  unconverged
  agent     s3  global      3/3  converged
  api       s1  replicated  2/2  converged

Unhealthy tasks:
  database.1  t1  database  worker-1  running  rejected  no suitable node
`, stdout.String())
}

func TestStatusJSONOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"status", "--json"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.JSONEq(t, `{
  "schema_version": 2,
  "health": "degraded",
  "findings": [
    {
      "code": "task_unhealthy",
      "message": "task database.1 is rejected: no suitable node"
    },
    {
      "code": "service_unconverged",
      "message": "service database has 0/1 running tasks"
    }
  ],
  "summary": {
    "healthy_managers": 2,
    "managers": 2,
    "ready_nodes": 3,
    "active_nodes": 3,
    "nodes": 3,
    "converged_services": 2,
    "services": 3
  },
  "endpoint": "unix:///var/run/docker.sock",
  "cluster": {
    "id": "cluster-1",
    "local_state": "active",
    "control_available": true
  },
  "leader": "manager-1",
  "nodes": [
    {
      "id": "m1",
      "hostname": "manager-1",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "leader"
    },
    {
      "id": "m2",
      "hostname": "manager-2",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "reachable"
    },
    {
      "id": "w1",
      "hostname": "worker-1",
      "role": "worker",
      "state": "ready",
      "availability": "active"
    }
  ],
  "services": [
    {
      "id": "s2",
      "name": "database",
      "mode": "replicated",
      "running_tasks": 0,
      "desired_tasks": 1,
      "converged": false
    },
    {
      "id": "s3",
      "name": "agent",
      "mode": "global",
      "running_tasks": 3,
      "desired_tasks": 3,
      "converged": true
    },
    {
      "id": "s1",
      "name": "api",
      "mode": "replicated",
      "running_tasks": 2,
      "desired_tasks": 2,
      "converged": true
    }
  ],
  "unhealthy_tasks": [
    {
      "id": "t1",
      "name": "database.1",
      "service": "database",
      "node": "worker-1",
      "desired_state": "running",
      "state": "rejected",
      "error": "no suitable node"
    }
  ]
	}`, stdout.String())
	assert.Equal(t, `{
  "schema_version": 2,
  "health": "degraded",
  "findings": [
    {
      "code": "task_unhealthy",
      "message": "task database.1 is rejected: no suitable node"
    },
    {
      "code": "service_unconverged",
      "message": "service database has 0/1 running tasks"
    }
  ],
  "summary": {
    "healthy_managers": 2,
    "managers": 2,
    "ready_nodes": 3,
    "active_nodes": 3,
    "nodes": 3,
    "converged_services": 2,
    "services": 3
  },
  "endpoint": "unix:///var/run/docker.sock",
  "cluster": {
    "id": "cluster-1",
    "local_state": "active",
    "control_available": true
  },
  "leader": "manager-1",
  "nodes": [
    {
      "id": "m1",
      "hostname": "manager-1",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "leader"
    },
    {
      "id": "m2",
      "hostname": "manager-2",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "reachable"
    },
    {
      "id": "w1",
      "hostname": "worker-1",
      "role": "worker",
      "state": "ready",
      "availability": "active"
    }
  ],
  "services": [
    {
      "id": "s2",
      "name": "database",
      "mode": "replicated",
      "running_tasks": 0,
      "desired_tasks": 1,
      "converged": false
    },
    {
      "id": "s3",
      "name": "agent",
      "mode": "global",
      "running_tasks": 3,
      "desired_tasks": 3,
      "converged": true
    },
    {
      "id": "s1",
      "name": "api",
      "mode": "replicated",
      "running_tasks": 2,
      "desired_tasks": 2,
      "converged": true
    }
  ],
  "unhealthy_tasks": [
    {
      "id": "t1",
      "name": "database.1",
      "service": "database",
      "node": "worker-1",
      "desired_state": "running",
      "state": "rejected",
      "error": "no suitable node"
    }
  ]
}
`, stdout.String())
}

func TestStatusReportsUnavailableSwarmFirst(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"status"}, &fakeConnector{connection: unavailableInspector{}}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `DEGRADED
  Swarm state is inactive, expected active
  Swarm control is unavailable

Managers: 0/0 healthy
Nodes: 0/0 ready, 0 active
Services: 0/0 converged
Cluster:
Endpoint: unix:///var/run/docker.sock
Swarm: inactive
Control: unavailable
`, stdout.String())
}

func TestStatusUsesExplicitDockerContext(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	exitCode := Run(context.Background(), []string{"--context", "swarm", "status"}, connector, &bytes.Buffer{}, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, "swarm", connector.contextName)
}

func TestServiceDiagnoseReportsCurrentFailure(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"service", "diagnose", "database"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `DEGRADED service database
Mode: replicated
Tasks: 0/1 running

Current failures:
  database.1  rejected  worker-1  no suitable node

Recent terminal tasks:
  database.1  failed  worker-1  old failure

Placement eligibility (readiness and availability only):
  manager-1  passes evaluated checks
  manager-2  passes evaluated checks
  worker-1   passes evaluated checks
Not evaluated: constraints, platform, resources, replica limits, ports, storage
`, stdout.String())
}

func TestServiceDiagnoseJSONOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"service", "diagnose", "database", "--json"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.JSONEq(t, `{
  "schema_version": 2,
  "health": "degraded",
  "service": {
    "id": "s2",
    "name": "database",
    "mode": "replicated",
    "running_tasks": 0,
    "desired_tasks": 1,
    "converged": false
  },
  "current_failures": [
    {
      "id": "t1",
      "name": "database.1",
      "service": "database",
      "node": "worker-1",
      "desired_state": "running",
      "state": "rejected",
      "error": "no suitable node"
    }
  ],
  "recent_terminal_tasks": [
    {
      "id": "old-t1",
      "name": "database.1",
      "service": "database",
      "node": "worker-1",
      "desired_state": "shutdown",
      "state": "failed",
      "error": "old failure"
    }
  ],
  "placement_eligibility": {
    "evaluated_inputs": [
      "node_readiness",
      "node_availability"
    ],
    "unevaluated_inputs": [
      "placement_constraints",
      "platform_requirements",
      "resource_reservations",
      "maximum_replicas_per_node",
      "host_published_port_conflicts",
      "storage_portability"
    ],
    "nodes": [
      {
        "id": "m1",
        "hostname": "manager-1",
        "passes_evaluated_checks": true,
        "blockers": []
      },
      {
        "id": "m2",
        "hostname": "manager-2",
        "passes_evaluated_checks": true,
        "blockers": []
      },
      {
        "id": "w1",
        "hostname": "worker-1",
        "passes_evaluated_checks": true,
        "blockers": []
      }
    ]
  }
}`, stdout.String())
	assert.NotContains(t, stdout.String(), "\x1b")
}

func TestServiceDiagnoseKeepsHistoricalFailureSeparateFromCurrentHealth(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"service", "diagnose", "s1"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, `HEALTHY service api
Mode: replicated
Tasks: 2/2 running

Current failures: none

Recent terminal tasks:
  api.1  failed  worker-1  old rollout failure

Placement eligibility (readiness and availability only):
  manager-1  passes evaluated checks
  manager-2  passes evaluated checks
  worker-1   passes evaluated checks
Not evaluated: constraints, platform, resources, replica limits, ports, storage
`, stdout.String())
}

func TestServiceDiagnoseReportsNodeReadinessAndAvailabilityEligibility(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{connection: resultInspector{result: status.Result{
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", State: "ready", Availability: "active"},
			{ID: "w1", Hostname: "worker-1", State: "down", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", State: "ready", Availability: "drain"},
		},
		Services: []status.Service{
			{ID: "s1", Name: "database", Mode: "replicated", DesiredTasks: 1},
		},
	}}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"service", "diagnose", "database"}, connector, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `DEGRADED service database
Mode: replicated
Tasks: 0/1 running

Current failures: none

Recent terminal tasks: none

Placement eligibility (readiness and availability only):
  manager-1  passes evaluated checks
  worker-1   blocked: state is down
  worker-2   blocked: availability is drain
Not evaluated: constraints, platform, resources, replica limits, ports, storage
`, stdout.String())

	stdout.Reset()
	exitCode = Run(context.Background(), []string{"service", "diagnose", "database", "--json"}, connector, &stdout, &bytes.Buffer{})
	assert.Equal(t, ExitSafetyGate, exitCode)
	var diagnosis status.ServiceDiagnosis
	assert.NoError(t, json.Unmarshal(stdout.Bytes(), &diagnosis))
	assert.Equal(t, status.ServiceDiagnosisSchemaVersion, diagnosis.SchemaVersion)
	assert.Equal(t, []status.PlacementBlocker{{
		Code:    "node_not_ready",
		Message: "state is down",
	}}, diagnosis.PlacementEligibility.Nodes[1].Blockers)
	assert.Equal(t, []status.PlacementBlocker{{
		Code:    "node_not_active",
		Message: "availability is drain",
	}}, diagnosis.PlacementEligibility.Nodes[2].Blockers)
}

func TestAssessHealthEvaluatesNodeAndManagerHealth(t *testing.T) {
	tests := []struct {
		name         string
		nodes        []status.Node
		wantHealth   status.Health
		wantFindings []status.HealthFinding
	}{
		{
			name: "healthy drained manager",
			nodes: []status.Node{
				{Hostname: "manager-1", Role: "manager", State: "ready", Availability: "drain", ManagerStatus: "leader"},
				{Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
			},
			wantHealth:   status.HealthHealthy,
			wantFindings: []status.HealthFinding{},
		},
		{
			name:       "manager status unavailable",
			nodes:      []status.Node{{Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active"}},
			wantHealth: status.HealthDegraded,
			wantFindings: []status.HealthFinding{{
				Code: "manager_unhealthy", Message: "manager manager-1 is unhealthy: manager status is unavailable, expected leader or reachable",
			}},
		},
		{
			name:       "worker down",
			nodes:      []status.Node{{Hostname: "worker-1", Role: "worker", State: "down", Availability: "active"}},
			wantHealth: status.HealthDegraded,
			wantFindings: []status.HealthFinding{{
				Code: "node_not_ready", Message: "node worker-1 state is down, expected ready",
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assessment := status.AssessHealth(status.Result{
				Cluster: status.Cluster{LocalState: "active", ControlAvailable: true},
				Nodes:   test.nodes,
			})

			assert.Equal(t, test.wantHealth, assessment.Health)
			assert.Equal(t, test.wantFindings, assessment.Findings)
		})
	}
}

type fakeConnector struct {
	contextName string
	connection  status.Connection
}

func (f *fakeConnector) Connect(_ context.Context, contextName string) (status.Connection, error) {
	f.contextName = contextName
	if f.connection == nil {
		f.connection = fakeInspector{}
	}
	return f.connection, nil
}

type fakeInspector struct{}

func (fakeInspector) Inspect(context.Context) (status.Result, error) {
	return status.Result{
		SchemaVersion: 1,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster: status.Cluster{
			ID:               "cluster-1",
			LocalState:       "active",
			ControlAvailable: true,
		},
		Leader: "manager-1",
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "s2", Name: "database", Mode: "replicated", RunningTasks: 0, DesiredTasks: 1, Converged: false},
			{ID: "s3", Name: "agent", Mode: "global", RunningTasks: 3, DesiredTasks: 3, Converged: true},
			{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 2, DesiredTasks: 2, Converged: true},
		},
		UnhealthyTasks: []status.Task{
			{ID: "t1", Name: "database.1", ServiceID: "s2", Service: "database", Node: "worker-1", DesiredState: "running", State: "rejected", Error: "no suitable node"},
		},
		Tasks: []status.Task{
			{ID: "old-api", Name: "api.1", ServiceID: "s1", Service: "api", Node: "worker-1", DesiredState: "shutdown", State: "failed", Error: "old rollout failure"},
			{ID: "old-t1", Name: "database.1", ServiceID: "s2", Service: "database", Node: "worker-1", DesiredState: "shutdown", State: "failed", Error: "old failure"},
		},
	}, nil
}

func (fakeInspector) Close() error { return nil }

type resultInspector struct {
	result status.Result
}

func (i resultInspector) Inspect(context.Context) (status.Result, error) { return i.result, nil }
func (resultInspector) Close() error                                     { return nil }

type unavailableInspector struct{}

func (unavailableInspector) Inspect(context.Context) (status.Result, error) {
	return status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "inactive"},
	}, nil
}

func (unavailableInspector) Close() error { return nil }
