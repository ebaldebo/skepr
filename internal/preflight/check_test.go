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
