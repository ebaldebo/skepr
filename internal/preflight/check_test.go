package preflight

import (
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
)

func TestCheckNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		requestedNode string
		nodes         []status.Node
		expected      Result
	}{
		{
			name:          "ready active node by hostname",
			requestedNode: "worker-1",
			nodes: []status.Node{
				{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
			},
			expected: Result{
				SchemaVersion: status.SchemaVersion,
				Endpoint:      "unix:///var/run/docker.sock",
				RequestedNode: "worker-1",
				Target:        &status.Node{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
				Safe:          true,
				Findings: []Finding{
					{Gate: "target_exists", Level: LevelPass, Message: "target node worker-1 exists with role worker"},
					{Gate: "target_ready", Level: LevelPass, Message: "target node worker-1 is ready"},
					{Gate: "target_active", Level: LevelPass, Message: "target node worker-1 is active"},
					{Gate: "swarm_active", Level: LevelPass, Message: "connected Docker endpoint is part of an active Swarm"},
					{Gate: "swarm_control_available", Level: LevelPass, Message: "connected Docker endpoint provides Swarm manager control"},
				},
			},
		},
		{
			name:          "unready paused node by ID",
			requestedNode: "w1",
			nodes: []status.Node{
				{ID: "w1", Hostname: "worker-1", Role: "worker", State: "down", Availability: "pause"},
			},
			expected: Result{
				SchemaVersion: status.SchemaVersion,
				Endpoint:      "unix:///var/run/docker.sock",
				RequestedNode: "w1",
				Target:        &status.Node{ID: "w1", Hostname: "worker-1", Role: "worker", State: "down", Availability: "pause"},
				Safe:          false,
				Findings: []Finding{
					{Gate: "target_exists", Level: LevelPass, Message: "target node worker-1 exists with role worker"},
					{Gate: "target_ready", Level: LevelBlocker, Message: "target node worker-1 state is down, expected ready"},
					{Gate: "target_active", Level: LevelBlocker, Message: "target node worker-1 availability is pause, expected active"},
					{Gate: "swarm_active", Level: LevelPass, Message: "connected Docker endpoint is part of an active Swarm"},
					{Gate: "swarm_control_available", Level: LevelPass, Message: "connected Docker endpoint provides Swarm manager control"},
				},
			},
		},
		{
			name:          "missing node",
			requestedNode: "missing",
			expected: Result{
				SchemaVersion: status.SchemaVersion,
				Endpoint:      "unix:///var/run/docker.sock",
				RequestedNode: "missing",
				Safe:          false,
				Findings: []Finding{
					{Gate: "target_exists", Level: LevelBlocker, Message: "target node missing was not found"},
					{Gate: "swarm_active", Level: LevelPass, Message: "connected Docker endpoint is part of an active Swarm"},
					{Gate: "swarm_control_available", Level: LevelPass, Message: "connected Docker endpoint provides Swarm manager control"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CheckNode(status.Result{
				SchemaVersion: status.SchemaVersion,
				Endpoint:      "unix:///var/run/docker.sock",
				Cluster:       status.Cluster{LocalState: "active", ControlAvailable: true},
				Nodes:         tt.nodes,
			}, tt.requestedNode)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckNodeEndpointGates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		cluster          status.Cluster
		expectedFindings []Finding
	}{
		{
			name:    "inactive Swarm",
			cluster: status.Cluster{LocalState: "inactive", ControlAvailable: true},
			expectedFindings: []Finding{
				{Gate: "swarm_active", Level: LevelBlocker, Message: "connected Docker endpoint Swarm state is inactive, expected active"},
				{Gate: "swarm_control_available", Level: LevelPass, Message: "connected Docker endpoint provides Swarm manager control"},
			},
		},
		{
			name:    "control unavailable",
			cluster: status.Cluster{LocalState: "active", ControlAvailable: false},
			expectedFindings: []Finding{
				{Gate: "swarm_active", Level: LevelPass, Message: "connected Docker endpoint is part of an active Swarm"},
				{Gate: "swarm_control_available", Level: LevelBlocker, Message: "connected Docker endpoint does not provide Swarm manager control"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CheckNode(status.Result{
				Cluster: tt.cluster,
				Nodes: []status.Node{
					{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
				},
			}, "worker-1")

			assert.False(t, result.Safe)
			assert.Equal(t, tt.expectedFindings, result.Findings[3:])
		})
	}
}

func TestCheckNodeManagerHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		manager         status.Node
		expectedSafe    bool
		expectedFinding Finding
	}{
		{
			name:            "healthy leader",
			manager:         status.Node{Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			expectedSafe:    true,
			expectedFinding: Finding{Gate: "manager_healthy", Level: LevelPass, Message: "Swarm manager manager-1 is healthy (ready, active and leader)"},
		},
		{
			name:            "healthy reachable manager",
			manager:         status.Node{Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			expectedSafe:    true,
			expectedFinding: Finding{Gate: "manager_healthy", Level: LevelPass, Message: "Swarm manager manager-2 is healthy (ready, active and reachable)"},
		},
		{
			name:            "unhealthy manager",
			manager:         status.Node{Hostname: "manager-2", Role: "manager", State: "down", Availability: "drain", ManagerStatus: "unreachable"},
			expectedSafe:    false,
			expectedFinding: Finding{Gate: "manager_healthy", Level: LevelBlocker, Message: "Swarm manager manager-2 is unhealthy: state is down, expected ready; availability is drain, expected active; manager status is unreachable, expected leader or reachable"},
		},
		{
			name:            "manager status unavailable",
			manager:         status.Node{Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active"},
			expectedSafe:    false,
			expectedFinding: Finding{Gate: "manager_healthy", Level: LevelBlocker, Message: "Swarm manager manager-2 is unhealthy: manager status is unavailable, expected leader or reachable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CheckNode(status.Result{
				Cluster: status.Cluster{LocalState: "active", ControlAvailable: true},
				Nodes: []status.Node{
					tt.manager,
					{Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
				},
			}, "worker-1")

			assert.Equal(t, tt.expectedSafe, result.Safe)
			assert.Equal(t, tt.expectedFinding, result.Findings[len(result.Findings)-1])
		})
	}
}

func TestCheckNodeTargetLeadership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		managerStatus   string
		expectedSafe    bool
		expectedFinding Finding
	}{
		{
			name:            "leader",
			managerStatus:   "leader",
			expectedSafe:    false,
			expectedFinding: Finding{Gate: "target_not_leader", Level: LevelBlocker, Message: "target manager manager-1 is the current Swarm leader"},
		},
		{
			name:            "reachable non-leader",
			managerStatus:   "reachable",
			expectedSafe:    true,
			expectedFinding: Finding{Gate: "target_not_leader", Level: LevelPass, Message: "target manager manager-1 is not the current Swarm leader"},
		},
		{
			name:            "leadership unavailable",
			expectedSafe:    false,
			expectedFinding: Finding{Gate: "target_not_leader", Level: LevelBlocker, Message: "target manager manager-1 leadership cannot be verified: manager status is unavailable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CheckNode(status.Result{
				Cluster: status.Cluster{LocalState: "active", ControlAvailable: true},
				Nodes: []status.Node{
					{Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: tt.managerStatus},
					{Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
					{Hostname: "manager-3", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
				},
			}, "manager-1")

			assert.Equal(t, tt.expectedSafe, result.Safe)
			assert.Equal(t, tt.expectedFinding, result.Findings[3])
		})
	}
}

func TestCheckNodeManagerQuorum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		nodes           []status.Node
		expectedSafe    bool
		expectedFinding Finding
	}{
		{
			name: "one manager",
			nodes: []status.Node{
				{ID: "target", Hostname: "manager-target", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			expectedSafe:    false,
			expectedFinding: Finding{Gate: "manager_quorum", Level: LevelBlocker, Message: "taking target manager manager-target offline leaves 0 healthy managers; quorum requires 1 of 1"},
		},
		{
			name: "two managers",
			nodes: []status.Node{
				{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
				{ID: "target", Hostname: "manager-target", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			expectedSafe:    false,
			expectedFinding: Finding{Gate: "manager_quorum", Level: LevelBlocker, Message: "taking target manager manager-target offline leaves 1 healthy manager; quorum requires 2 of 2"},
		},
		{
			name: "three healthy managers",
			nodes: []status.Node{
				{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
				{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
				{ID: "target", Hostname: "manager-target", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			expectedSafe:    true,
			expectedFinding: Finding{Gate: "manager_quorum", Level: LevelPass, Message: "taking target manager manager-target offline leaves 2 healthy managers; quorum requires 2 of 3"},
		},
		{
			name: "three managers with one unhealthy remainder",
			nodes: []status.Node{
				{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
				{ID: "m2", Hostname: "manager-2", Role: "manager", State: "down", Availability: "active", ManagerStatus: "unreachable"},
				{ID: "target", Hostname: "manager-target", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			expectedSafe:    false,
			expectedFinding: Finding{Gate: "manager_quorum", Level: LevelBlocker, Message: "taking target manager manager-target offline leaves 1 healthy manager; quorum requires 2 of 3"},
		},
		{
			name: "five healthy managers",
			nodes: []status.Node{
				{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
				{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
				{ID: "m3", Hostname: "manager-3", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
				{ID: "m4", Hostname: "manager-4", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
				{ID: "target", Hostname: "manager-target", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			expectedSafe:    true,
			expectedFinding: Finding{Gate: "manager_quorum", Level: LevelPass, Message: "taking target manager manager-target offline leaves 4 healthy managers; quorum requires 3 of 5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CheckNode(status.Result{
				Cluster: status.Cluster{LocalState: "active", ControlAvailable: true},
				Nodes:   tt.nodes,
			}, "target")

			assert.Equal(t, tt.expectedSafe, result.Safe)
			assert.Equal(t, tt.expectedFinding, result.Findings[4])
		})
	}
}

func TestCheckNodeServiceConvergence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		services         []status.Service
		expectedSafe     bool
		expectedFindings []Finding
	}{
		{
			name:         "no services",
			expectedSafe: true,
		},
		{
			name: "one converged service",
			services: []status.Service{
				{Name: "api", RunningTasks: 2, DesiredTasks: 2, Converged: true},
			},
			expectedSafe: true,
			expectedFindings: []Finding{
				{Gate: "service_converged", Level: LevelPass, Message: "Swarm service api is converged"},
			},
		},
		{
			name: "multiple converged services",
			services: []status.Service{
				{Name: "api", RunningTasks: 2, DesiredTasks: 2, Converged: true},
				{Name: "database", RunningTasks: 1, DesiredTasks: 1, Converged: true},
			},
			expectedSafe: true,
			expectedFindings: []Finding{
				{Gate: "service_converged", Level: LevelPass, Message: "all 2 Swarm services are converged"},
			},
		},
		{
			name: "unconverged service",
			services: []status.Service{
				{Name: "api", RunningTasks: 2, DesiredTasks: 2, Converged: true},
				{Name: "database", RunningTasks: 0, DesiredTasks: 1, Converged: false},
			},
			expectedSafe: false,
			expectedFindings: []Finding{
				{Gate: "service_converged", Level: LevelBlocker, Message: "Swarm service database has 0 of 1 running tasks"},
			},
		},
		{
			name: "multiple unconverged services",
			services: []status.Service{
				{Name: "api", RunningTasks: 1, DesiredTasks: 2, Converged: false},
				{Name: "database", RunningTasks: 0, DesiredTasks: 1, Converged: false},
			},
			expectedSafe: false,
			expectedFindings: []Finding{
				{Gate: "service_converged", Level: LevelBlocker, Message: "Swarm service api has 1 of 2 running tasks"},
				{Gate: "service_converged", Level: LevelBlocker, Message: "Swarm service database has 0 of 1 running tasks"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CheckNode(status.Result{
				Cluster: status.Cluster{LocalState: "active", ControlAvailable: true},
				Nodes: []status.Node{
					{Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
				},
				Services: tt.services,
			}, "worker-1")

			var serviceFindings []Finding
			for _, finding := range result.Findings {
				if finding.Gate == "service_converged" {
					serviceFindings = append(serviceFindings, finding)
				}
			}
			assert.Equal(t, tt.expectedSafe, result.Safe)
			assert.Equal(t, tt.expectedFindings, serviceFindings)
		})
	}
}
