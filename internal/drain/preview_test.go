package drain

import (
	"testing"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPreviewSeparatesManagerDrainAndOfflineSafety(t *testing.T) {
	tests := []struct {
		name             string
		target           string
		nodes            []status.Node
		wantOfflineSafe  bool
		wantOfflineGate  string
		wantOfflineLevel preflight.Level
	}{
		{
			name:   "leader can drain but cannot go offline",
			target: "manager-1",
			nodes: []status.Node{
				{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
				{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
				{ID: "m3", Hostname: "manager-3", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			wantOfflineGate:  "target_not_leader",
			wantOfflineLevel: preflight.LevelBlocker,
		},
		{
			name:   "reachable manager in three manager swarm can go offline",
			target: "manager-2",
			nodes: []status.Node{
				{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
				{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
				{ID: "m3", Hostname: "manager-3", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			wantOfflineSafe:  true,
			wantOfflineGate:  "manager_quorum",
			wantOfflineLevel: preflight.LevelPass,
		},
		{
			name:   "reachable manager in two manager swarm cannot go offline",
			target: "manager-2",
			nodes: []status.Node{
				{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
				{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			},
			wantOfflineGate:  "manager_quorum",
			wantOfflineLevel: preflight.LevelBlocker,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			preview := BuildPreview(status.Result{
				Cluster: status.Cluster{LocalState: "active", ControlAvailable: true},
				Nodes:   test.nodes,
			}, test.target)

			assert.True(t, preview.SafeToDrain)
			assert.Equal(t, test.wantOfflineSafe, preview.SafeToTakeOffline)
			require.NotEmpty(t, preview.ManagerOfflineFindings)
			var matched *preflight.Finding
			for index := range preview.ManagerOfflineFindings {
				if preview.ManagerOfflineFindings[index].Gate == test.wantOfflineGate {
					matched = &preview.ManagerOfflineFindings[index]
				}
			}
			require.NotNil(t, matched)
			assert.Equal(t, test.wantOfflineLevel, matched.Level)
		})
	}
}

func TestBuildPreviewRequiresCapacityForEveryMovingTask(t *testing.T) {
	t.Parallel()

	tasks := []status.Task{
		{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t2", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t3", Name: "api.3", ServiceID: "s1", Service: "api", NodeID: "w2", Node: "worker-2", DesiredState: "running", State: "running"},
	}
	preview := BuildPreview(status.Result{
		Cluster: status.Cluster{LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", Role: "worker", State: "ready", Availability: "active"},
		},
		Services:     []status.Service{{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 3, DesiredTasks: 3, Converged: true, MaxReplicasPerNode: 2}},
		DesiredTasks: tasks,
		Tasks:        tasks,
	}, "worker-1")

	assert.False(t, preview.SafeToDrain)
	require.Len(t, preview.ServiceImpacts, 1)
	assert.Equal(t, 2, preview.ServiceImpacts[0].TaskCount)
	assert.Equal(t, 1, preview.ServiceImpacts[0].EstimatedTaskCapacity)
	assert.Contains(t, preview.DrainFindings, preflight.Finding{
		Gate:    "destination_capacity",
		Level:   preflight.LevelBlocker,
		Message: "service api has estimated destination capacity for 1 of 2 moving tasks based on evaluated placement inputs",
	})
}
