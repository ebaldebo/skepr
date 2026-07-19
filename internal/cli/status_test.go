package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

Placement eligibility:
  manager-1  passes evaluated checks
  manager-2  passes evaluated checks
  worker-1   passes evaluated checks
Evaluated constraints: none
Required platforms: any
Required resources: none
Maximum replicas per node: unlimited
Required host ports: none
Unevaluated constraints: none
Other inputs not evaluated: generic resources, storage
`, stdout.String())
}

func TestServiceDiagnoseJSONOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"service", "diagnose", "database", "--json"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.JSONEq(t, `{
  "schema_version": 7,
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
      "node_availability",
      "placement_constraints",
      "platform_requirements",
      "cpu_memory_reservations",
      "maximum_replicas_per_node",
      "host_published_port_conflicts"
    ],
    "unevaluated_inputs": [
      "generic_resources",
      "storage_portability"
    ],
    "evaluated_constraints": [],
    "unevaluated_constraints": [],
    "required_platforms": [],
    "required_resources": {
      "nano_cpus": 0,
      "memory_bytes": 0
    },
    "max_replicas_per_node": 0,
    "required_host_ports": [],
    "nodes": [
      {
        "id": "m1",
        "hostname": "manager-1",
        "passes_evaluated_checks": true,
        "active_service_tasks": 0,
        "used_host_ports": [],
        "blockers": []
      },
      {
        "id": "m2",
        "hostname": "manager-2",
        "passes_evaluated_checks": true,
        "active_service_tasks": 0,
        "used_host_ports": [],
        "blockers": []
      },
      {
        "id": "w1",
        "hostname": "worker-1",
        "passes_evaluated_checks": true,
        "active_service_tasks": 0,
        "used_host_ports": [],
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

Placement eligibility:
  manager-1  passes evaluated checks
  manager-2  passes evaluated checks
  worker-1   passes evaluated checks
Evaluated constraints: none
Required platforms: any
Required resources: none
Maximum replicas per node: unlimited
Required host ports: none
Unevaluated constraints: none
Other inputs not evaluated: generic resources, storage
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

Placement eligibility:
  manager-1  passes evaluated checks
  worker-1   blocked: state is down
  worker-2   blocked: availability is drain
Evaluated constraints: none
Required platforms: any
Required resources: none
Maximum replicas per node: unlimited
Required host ports: none
Unevaluated constraints: none
Other inputs not evaluated: generic resources, storage
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

func TestServiceDiagnoseReportsPlacementConstraintEligibility(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{connection: resultInspector{result: status.Result{
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-east", State: "ready", Availability: "active", Labels: map[string]string{"region": "east"}},
			{ID: "w2", Hostname: "worker-west", State: "ready", Availability: "active", Labels: map[string]string{"region": "west"}},
			{ID: "w3", Hostname: "worker-unlabeled", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "s1", Name: "database", Mode: "replicated", DesiredTasks: 1, PlacementConstraints: []string{"node.labels.region==east", "engine.labels.storage==ssd"}},
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

Placement eligibility:
  worker-east       passes evaluated checks
  worker-unlabeled  blocked: constraint node.labels.region==east does not match
  worker-west       blocked: constraint node.labels.region==east does not match
Evaluated constraints: node.labels.region==east
Required platforms: any
Required resources: none
Maximum replicas per node: unlimited
Required host ports: none
Unevaluated constraints: engine.labels.storage==ssd
Other inputs not evaluated: generic resources, storage
`, stdout.String())
}

func TestServiceDiagnosisEvaluatesSupportedPlacementConstraints(t *testing.T) {
	tests := []struct {
		name            string
		constraint      string
		node            status.Node
		wantPass        bool
		wantUnevaluated []string
	}{
		{name: "node ID equality", constraint: "node.id==w1", node: status.Node{ID: "w1"}, wantPass: true},
		{name: "hostname mismatch", constraint: "node.hostname==worker-2", node: status.Node{Hostname: "worker-1"}},
		{name: "role inequality", constraint: "node.role!=manager", node: status.Node{Role: "worker"}, wantPass: true},
		{name: "label inequality mismatch", constraint: "node.labels.storage!=ssd", node: status.Node{Labels: map[string]string{"storage": "ssd"}}},
		{name: "missing label satisfies inequality", constraint: "node.labels.storage!=ssd", node: status.Node{}, wantPass: true},
		{name: "platform OS mismatch", constraint: "node.platform.os==linux", node: status.Node{Platform: status.Platform{OS: "darwin"}}},
		{name: "platform architecture equality", constraint: "node.platform.arch==x86_64", node: status.Node{Platform: status.Platform{Architecture: "x86_64"}}, wantPass: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.node.State = "ready"
			test.node.Availability = "active"
			diagnosis, found := status.DiagnoseService(status.Result{
				Nodes:    []status.Node{test.node},
				Services: []status.Service{{ID: "s1", PlacementConstraints: []string{test.constraint}}},
			}, "s1")

			require.True(t, found)
			require.Len(t, diagnosis.PlacementEligibility.Nodes, 1)
			assert.Equal(t, test.wantPass, diagnosis.PlacementEligibility.Nodes[0].PassesEvaluatedChecks)
			wantUnevaluated := test.wantUnevaluated
			if wantUnevaluated == nil {
				wantUnevaluated = []string{}
			}
			assert.Equal(t, wantUnevaluated, diagnosis.PlacementEligibility.UnevaluatedConstraints)
		})
	}
}

func TestServiceDiagnoseReportsPlatformEligibility(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{connection: resultInspector{result: status.Result{
		Nodes: []status.Node{
			{ID: "w1", Hostname: "linux-x86", State: "ready", Availability: "active", Platform: status.Platform{OS: "linux", Architecture: "x86_64"}},
			{ID: "w2", Hostname: "linux-arm", State: "ready", Availability: "active", Platform: status.Platform{OS: "linux", Architecture: "arm64"}},
			{ID: "w3", Hostname: "mac", State: "ready", Availability: "active", Platform: status.Platform{OS: "darwin", Architecture: "amd64"}},
		},
		Services: []status.Service{
			{ID: "s1", Name: "database", Mode: "replicated", DesiredTasks: 1, RequiredPlatforms: []status.Platform{{OS: "linux", Architecture: "amd64"}}},
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

Placement eligibility:
  linux-arm  blocked: platform linux/arm64 does not match required linux/amd64
  linux-x86  passes evaluated checks
  mac        blocked: platform darwin/amd64 does not match required linux/amd64
Evaluated constraints: none
Required platforms: linux/amd64
Required resources: none
Maximum replicas per node: unlimited
Required host ports: none
Unevaluated constraints: none
Other inputs not evaluated: generic resources, storage
`, stdout.String())

	stdout.Reset()
	exitCode = Run(context.Background(), []string{"service", "diagnose", "database", "--json"}, connector, &stdout, &bytes.Buffer{})
	assert.Equal(t, ExitSafetyGate, exitCode)
	var diagnosis status.ServiceDiagnosis
	assert.NoError(t, json.Unmarshal(stdout.Bytes(), &diagnosis))
	assert.Equal(t, status.ServiceDiagnosisSchemaVersion, diagnosis.SchemaVersion)
	assert.Equal(t, []status.Platform{{OS: "linux", Architecture: "amd64"}}, diagnosis.PlacementEligibility.RequiredPlatforms)
	assert.Equal(t, []status.PlacementBlocker{{
		Code:    "platform_mismatch",
		Message: "platform linux/arm64 does not match required linux/amd64",
	}}, diagnosis.PlacementEligibility.Nodes[0].Blockers)
	assert.True(t, diagnosis.PlacementEligibility.Nodes[1].PassesEvaluatedChecks)
}

func TestServiceDiagnoseReportsCPUAndMemoryEligibility(t *testing.T) {
	t.Parallel()

	gibibyte := int64(1 << 30)
	connector := &fakeConnector{connection: resultInspector{result: status.Result{
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-free", State: "ready", Availability: "active", Resources: status.Resources{NanoCPUs: 2_000_000_000, MemoryBytes: 4 * gibibyte}},
			{ID: "w2", Hostname: "worker-used", State: "ready", Availability: "active", Resources: status.Resources{NanoCPUs: 4_000_000_000, MemoryBytes: 8 * gibibyte}},
			{ID: "w3", Hostname: "worker-small", State: "ready", Availability: "active", Resources: status.Resources{NanoCPUs: 1_000_000_000, MemoryBytes: gibibyte}},
		},
		Services: []status.Service{
			{ID: "s1", Name: "database", Mode: "replicated", DesiredTasks: 1, Reservations: status.Resources{NanoCPUs: 2_000_000_000, MemoryBytes: 2 * gibibyte}},
		},
		Tasks: []status.Task{
			{ID: "t1", NodeID: "w2", State: "running", Reservations: status.Resources{NanoCPUs: 3_000_000_000, MemoryBytes: 7 * gibibyte}},
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

Placement eligibility:
  worker-free   passes evaluated checks
  worker-small  blocked: requires 2 CPUs, 1 CPU available; requires 2 GiB memory, 1 GiB available
  worker-used   blocked: requires 2 CPUs, 1 CPU available; requires 2 GiB memory, 1 GiB available
Evaluated constraints: none
Required platforms: any
Required resources: 2 CPUs, 2 GiB memory
Maximum replicas per node: unlimited
Required host ports: none
Unevaluated constraints: none
Other inputs not evaluated: generic resources, storage
`, stdout.String())

	stdout.Reset()
	exitCode = Run(context.Background(), []string{"service", "diagnose", "database", "--json"}, connector, &stdout, &bytes.Buffer{})
	assert.Equal(t, ExitSafetyGate, exitCode)
	var diagnosis status.ServiceDiagnosis
	assert.NoError(t, json.Unmarshal(stdout.Bytes(), &diagnosis))
	assert.Equal(t, status.Resources{NanoCPUs: 2_000_000_000, MemoryBytes: 2 * gibibyte}, diagnosis.PlacementEligibility.RequiredResources)
	assert.Equal(t, status.NodeResources{
		Capacity:  status.Resources{NanoCPUs: 4_000_000_000, MemoryBytes: 8 * gibibyte},
		Reserved:  status.Resources{NanoCPUs: 3_000_000_000, MemoryBytes: 7 * gibibyte},
		Available: status.Resources{NanoCPUs: 1_000_000_000, MemoryBytes: gibibyte},
	}, diagnosis.PlacementEligibility.Nodes[2].Resources)
	assert.Equal(t, []string{"insufficient_cpu", "insufficient_memory"}, []string{
		diagnosis.PlacementEligibility.Nodes[2].Blockers[0].Code,
		diagnosis.PlacementEligibility.Nodes[2].Blockers[1].Code,
	})
}

func TestServiceDiagnosisCountsActiveAssignedTaskReservations(t *testing.T) {
	tests := []struct {
		name         string
		task         status.Task
		wantReserved status.Resources
	}{
		{name: "running assigned task", task: status.Task{NodeID: "w1", State: "running", Reservations: status.Resources{NanoCPUs: 1_000_000_000}}, wantReserved: status.Resources{NanoCPUs: 1_000_000_000}},
		{name: "preparing assigned task", task: status.Task{NodeID: "w1", State: "preparing", Reservations: status.Resources{MemoryBytes: 1 << 30}}, wantReserved: status.Resources{MemoryBytes: 1 << 30}},
		{name: "failed task", task: status.Task{NodeID: "w1", State: "failed", Reservations: status.Resources{NanoCPUs: 1_000_000_000}}},
		{name: "unassigned task", task: status.Task{State: "running", Reservations: status.Resources{NanoCPUs: 1_000_000_000}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnosis, found := status.DiagnoseService(status.Result{
				Nodes:    []status.Node{{ID: "w1", State: "ready", Availability: "active", Resources: status.Resources{NanoCPUs: 4_000_000_000, MemoryBytes: 4 << 30}}},
				Services: []status.Service{{ID: "s1", Converged: true}},
				Tasks:    []status.Task{test.task},
			}, "s1")

			require.True(t, found)
			require.Len(t, diagnosis.PlacementEligibility.Nodes, 1)
			assert.Equal(t, test.wantReserved, diagnosis.PlacementEligibility.Nodes[0].Resources.Reserved)
		})
	}
}

func TestServiceDiagnoseReportsMaximumReplicasPerNodeEligibility(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{connection: resultInspector{result: status.Result{
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", State: "ready", Availability: "active"},
			{ID: "w3", Hostname: "worker-3", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 1, DesiredTasks: 3, MaxReplicasPerNode: 1},
		},
		Tasks: []status.Task{
			{ID: "t1", ServiceID: "s1", NodeID: "w1", State: "running"},
			{ID: "t2", ServiceID: "s1", NodeID: "w2", State: "preparing"},
			{ID: "old", ServiceID: "s1", NodeID: "w3", DesiredState: "running", State: "failed"},
			{ID: "other", ServiceID: "s2", NodeID: "w3", State: "running"},
		},
	}}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"service", "diagnose", "api"}, connector, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `DEGRADED service api
Mode: replicated
Tasks: 1/3 running

Current failures: none

Recent terminal tasks: none

Placement eligibility:
  worker-1  blocked: service already has 1 active task, limit is 1
  worker-2  blocked: service already has 1 active task, limit is 1
  worker-3  passes evaluated checks
Evaluated constraints: none
Required platforms: any
Required resources: none
Maximum replicas per node: 1
Required host ports: none
Unevaluated constraints: none
Other inputs not evaluated: generic resources, storage
`, stdout.String())

	stdout.Reset()
	exitCode = Run(context.Background(), []string{"service", "diagnose", "api", "--json"}, connector, &stdout, &bytes.Buffer{})
	assert.Equal(t, ExitSafetyGate, exitCode)
	var diagnosis status.ServiceDiagnosis
	assert.NoError(t, json.Unmarshal(stdout.Bytes(), &diagnosis))
	assert.Equal(t, uint64(1), diagnosis.PlacementEligibility.MaxReplicasPerNode)
	assert.Equal(t, uint64(1), diagnosis.PlacementEligibility.Nodes[0].ActiveServiceTasks)
	assert.Equal(t, "max_replicas_per_node", diagnosis.PlacementEligibility.Nodes[0].Blockers[0].Code)
	assert.Equal(t, uint64(0), diagnosis.PlacementEligibility.Nodes[2].ActiveServiceTasks)
	assert.True(t, diagnosis.PlacementEligibility.Nodes[2].PassesEvaluatedChecks)
}

func TestServiceDiagnoseReportsHostPortEligibility(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{connection: resultInspector{result: status.Result{
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", State: "ready", Availability: "active"},
			{ID: "w3", Hostname: "worker-3", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "s1", Name: "api", Mode: "replicated", DesiredTasks: 1, HostPorts: []status.HostPort{{Protocol: "tcp", PublishedPort: 8080}, {Protocol: "udp", PublishedPort: 5353}}},
		},
		Tasks: []status.Task{
			{ID: "t1", ServiceID: "s2", NodeID: "w1", State: "running", HostPorts: []status.HostPort{{Protocol: "tcp", PublishedPort: 8080}}},
			{ID: "t2", ServiceID: "s3", NodeID: "w2", State: "preparing", HostPorts: []status.HostPort{{Protocol: "udp", PublishedPort: 5353}}},
			{ID: "old", ServiceID: "s4", NodeID: "w3", State: "failed", HostPorts: []status.HostPort{{Protocol: "tcp", PublishedPort: 8080}}},
		},
	}}}
	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"service", "diagnose", "api"}, connector, &stdout, &bytes.Buffer{})

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, `DEGRADED service api
Mode: replicated
Tasks: 0/1 running

Current failures: none

Recent terminal tasks: none

Placement eligibility:
  worker-1  blocked: host port 8080/tcp is already in use
  worker-2  blocked: host port 5353/udp is already in use
  worker-3  passes evaluated checks
Evaluated constraints: none
Required platforms: any
Required resources: none
Maximum replicas per node: unlimited
Required host ports: 5353/udp, 8080/tcp
Unevaluated constraints: none
Other inputs not evaluated: generic resources, storage
`, stdout.String())

	stdout.Reset()
	exitCode = Run(context.Background(), []string{"service", "diagnose", "api", "--json"}, connector, &stdout, &bytes.Buffer{})
	assert.Equal(t, ExitSafetyGate, exitCode)
	var diagnosis status.ServiceDiagnosis
	assert.NoError(t, json.Unmarshal(stdout.Bytes(), &diagnosis))
	assert.Equal(t, []status.HostPort{{Protocol: "udp", PublishedPort: 5353}, {Protocol: "tcp", PublishedPort: 8080}}, diagnosis.PlacementEligibility.RequiredHostPorts)
	assert.Equal(t, []status.HostPort{{Protocol: "tcp", PublishedPort: 8080}}, diagnosis.PlacementEligibility.Nodes[0].UsedHostPorts)
	assert.Equal(t, "host_port_conflict", diagnosis.PlacementEligibility.Nodes[0].Blockers[0].Code)
	assert.Equal(t, []status.HostPort{{Protocol: "udp", PublishedPort: 5353}}, diagnosis.PlacementEligibility.Nodes[1].UsedHostPorts)
	assert.Equal(t, "host_port_conflict", diagnosis.PlacementEligibility.Nodes[1].Blockers[0].Code)
	assert.Empty(t, diagnosis.PlacementEligibility.Nodes[2].UsedHostPorts)
	assert.True(t, diagnosis.PlacementEligibility.Nodes[2].PassesEvaluatedChecks)
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
