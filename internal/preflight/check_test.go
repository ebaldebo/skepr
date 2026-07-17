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
					{Gate: "target_exists", Level: LevelPass, Message: "node worker-1 exists"},
					{Gate: "target_ready", Level: LevelPass, Message: "node worker-1 is ready"},
					{Gate: "target_active", Level: LevelPass, Message: "node worker-1 is active"},
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
					{Gate: "target_exists", Level: LevelPass, Message: "node worker-1 exists"},
					{Gate: "target_ready", Level: LevelBlocker, Message: "node worker-1 state is down, expected ready"},
					{Gate: "target_active", Level: LevelBlocker, Message: "node worker-1 availability is pause, expected active"},
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
					{Gate: "target_exists", Level: LevelBlocker, Message: "node missing was not found"},
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
				Nodes:         tt.nodes,
			}, tt.requestedNode)

			assert.Equal(t, tt.expected, result)
		})
	}
}
