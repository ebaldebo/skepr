package docker

import (
	"context"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/api/types/swarm"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInspectorNormalizesSwarmStatus(t *testing.T) {
	t.Parallel()

	inspector := newInspector(fakeEngine{
		host: "unix:///var/run/docker.sock",
		info: system.Info{Swarm: swarm.Info{
			LocalNodeState:   swarm.LocalNodeStateActive,
			ControlAvailable: true,
			Cluster:          &swarm.ClusterInfo{ID: "cluster-1"},
		}},
		nodes: []swarm.Node{
			{
				ID:          "w1",
				Spec:        swarm.NodeSpec{Role: swarm.NodeRoleWorker, Availability: swarm.NodeAvailabilityActive},
				Description: swarm.NodeDescription{Hostname: "worker-1"},
				Status:      swarm.NodeStatus{State: swarm.NodeStateReady},
			},
			{
				ID:            "m2",
				Spec:          swarm.NodeSpec{Role: swarm.NodeRoleManager, Availability: swarm.NodeAvailabilityActive},
				Description:   swarm.NodeDescription{Hostname: "manager-2"},
				Status:        swarm.NodeStatus{State: swarm.NodeStateReady},
				ManagerStatus: &swarm.ManagerStatus{Reachability: swarm.ReachabilityReachable},
			},
			{
				ID:            "m1",
				Spec:          swarm.NodeSpec{Role: swarm.NodeRoleManager, Availability: swarm.NodeAvailabilityActive},
				Description:   swarm.NodeDescription{Hostname: "manager-1"},
				Status:        swarm.NodeStatus{State: swarm.NodeStateReady},
				ManagerStatus: &swarm.ManagerStatus{Leader: true, Reachability: swarm.ReachabilityReachable},
			},
		},
	})

	result, err := inspector.Inspect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, status.Result{
		SchemaVersion: status.SchemaVersion,
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
	}, result)
}

type fakeEngine struct {
	host  string
	info  system.Info
	nodes []swarm.Node
}

func (f fakeEngine) NodeList(context.Context, client.NodeListOptions) (client.NodeListResult, error) {
	return client.NodeListResult{Items: f.nodes}, nil
}

func (f fakeEngine) Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error) {
	return client.SystemInfoResult{Info: f.info}, nil
}

func (f fakeEngine) DaemonHost() string {
	return f.host
}
