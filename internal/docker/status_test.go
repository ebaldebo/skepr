package docker

import (
	"context"
	"errors"
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
		services: []swarm.Service{
			{
				ID: "s1",
				Spec: swarm.ServiceSpec{
					Annotations: swarm.Annotations{Name: "api"},
					Mode:        swarm.ServiceMode{Replicated: &swarm.ReplicatedService{Replicas: uint64Pointer(2)}},
				},
				ServiceStatus: &swarm.ServiceStatus{RunningTasks: 2, DesiredTasks: 2},
			},
			{
				ID: "s3",
				Spec: swarm.ServiceSpec{
					Annotations: swarm.Annotations{Name: "agent"},
					Mode:        swarm.ServiceMode{Global: &swarm.GlobalService{}},
				},
				ServiceStatus: &swarm.ServiceStatus{RunningTasks: 3, DesiredTasks: 3},
			},
			{
				ID: "s2",
				Spec: swarm.ServiceSpec{
					Annotations: swarm.Annotations{Name: "database"},
					Mode:        swarm.ServiceMode{Replicated: &swarm.ReplicatedService{Replicas: uint64Pointer(1)}},
				},
				ServiceStatus: &swarm.ServiceStatus{RunningTasks: 0, DesiredTasks: 1},
			},
		},
		tasks: []swarm.Task{
			{ID: "healthy", ServiceID: "s1", Slot: 1, NodeID: "w1", DesiredState: swarm.TaskStateRunning, Status: swarm.TaskStatus{State: swarm.TaskStateRunning}},
			{ID: "starting", ServiceID: "s1", Slot: 2, NodeID: "w1", DesiredState: swarm.TaskStateRunning, Status: swarm.TaskStatus{State: swarm.TaskStateStarting}},
			{ID: "rejected", ServiceID: "s2", Slot: 1, NodeID: "w1", DesiredState: swarm.TaskStateRunning, Status: swarm.TaskStatus{State: swarm.TaskStateRejected, Err: "no suitable node"}},
			{ID: "failed", ServiceID: "s1", Slot: 2, NodeID: "w1", DesiredState: swarm.TaskStateRunning, Status: swarm.TaskStatus{State: swarm.TaskStateFailed, Err: "exit code 1"}},
			{ID: "orphaned", ServiceID: "s3", NodeID: "m2", DesiredState: swarm.TaskStateRunning, Status: swarm.TaskStatus{State: swarm.TaskStateOrphaned}},
			{ID: "historical", ServiceID: "s2", Slot: 1, NodeID: "w1", DesiredState: swarm.TaskStateShutdown, Status: swarm.TaskStatus{State: swarm.TaskStateFailed, Err: "old failure"}},
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
		Services: []status.Service{
			{ID: "s2", Name: "database", Mode: "replicated", RunningTasks: 0, DesiredTasks: 1, Converged: false},
			{ID: "s3", Name: "agent", Mode: "global", RunningTasks: 3, DesiredTasks: 3, Converged: true},
			{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 2, DesiredTasks: 2, Converged: true},
		},
		UnhealthyTasks: []status.Task{
			{ID: "orphaned", Name: "agent.manager-2", ServiceID: "s3", Service: "agent", NodeID: "m2", Node: "manager-2", DesiredState: "running", State: "orphaned"},
			{ID: "failed", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "failed", Error: "exit code 1"},
			{ID: "rejected", Name: "database.1", ServiceID: "s2", Service: "database", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "rejected", Error: "no suitable node"},
		},
		DesiredTasks: []status.Task{
			{ID: "orphaned", Name: "agent.manager-2", ServiceID: "s3", Service: "agent", NodeID: "m2", Node: "manager-2", DesiredState: "running", State: "orphaned"},
			{ID: "healthy", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "failed", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "failed", Error: "exit code 1"},
			{ID: "starting", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "starting"},
			{ID: "rejected", Name: "database.1", ServiceID: "s2", Service: "database", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "rejected", Error: "no suitable node"},
		},
	}, result)
}

func TestInspectorRejectsServiceWithoutTaskCounts(t *testing.T) {
	t.Parallel()

	inspector := newInspector(fakeEngine{
		host: "unix:///var/run/docker.sock",
		info: system.Info{Swarm: swarm.Info{
			LocalNodeState:   swarm.LocalNodeStateActive,
			ControlAvailable: true,
		}},
		services: []swarm.Service{{
			ID: "s1",
			Spec: swarm.ServiceSpec{
				Annotations: swarm.Annotations{Name: "api"},
				Mode:        swarm.ServiceMode{Replicated: &swarm.ReplicatedService{}},
			},
		}},
	})

	_, err := inspector.Inspect(context.Background())

	require.EqualError(t, err, `query Swarm services at "unix:///var/run/docker.sock": service "api" has no task counts`)
}

func TestInspectorNormalizesDesiredRunningTasks(t *testing.T) {
	t.Parallel()

	inspector := newInspector(fakeEngine{
		host: "unix:///var/run/docker.sock",
		info: system.Info{Swarm: swarm.Info{
			LocalNodeState:   swarm.LocalNodeStateActive,
			ControlAvailable: true,
		}},
		nodes: []swarm.Node{
			{
				ID:          "w1",
				Spec:        swarm.NodeSpec{Role: swarm.NodeRoleWorker, Availability: swarm.NodeAvailabilityActive},
				Description: swarm.NodeDescription{Hostname: "worker-1"},
				Status:      swarm.NodeStatus{State: swarm.NodeStateReady},
			},
		},
		services: []swarm.Service{
			{
				ID: "s1",
				Spec: swarm.ServiceSpec{
					Annotations: swarm.Annotations{Name: "api"},
					Mode:        swarm.ServiceMode{Replicated: &swarm.ReplicatedService{Replicas: uint64Pointer(2)}},
				},
				ServiceStatus: &swarm.ServiceStatus{RunningTasks: 2, DesiredTasks: 2},
			},
		},
		tasks: []swarm.Task{
			{ID: "preparing", ServiceID: "s1", Slot: 2, NodeID: "w1", DesiredState: swarm.TaskStateRunning, Status: swarm.TaskStatus{State: swarm.TaskStatePreparing}},
			{ID: "running", ServiceID: "s1", Slot: 1, NodeID: "w1", DesiredState: swarm.TaskStateRunning, Status: swarm.TaskStatus{State: swarm.TaskStateRunning}},
			{ID: "historical", ServiceID: "s1", Slot: 1, NodeID: "w1", DesiredState: swarm.TaskStateShutdown, Status: swarm.TaskStatus{State: swarm.TaskStateFailed}},
		},
	})

	result, err := inspector.Inspect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []status.Task{
		{ID: "running", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "preparing", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "preparing"},
	}, result.DesiredTasks)
}

type fakeEngine struct {
	host     string
	info     system.Info
	nodes    []swarm.Node
	services []swarm.Service
	tasks    []swarm.Task
}

func (f fakeEngine) NodeList(context.Context, client.NodeListOptions) (client.NodeListResult, error) {
	return client.NodeListResult{Items: f.nodes}, nil
}

func (f fakeEngine) ServiceList(_ context.Context, options client.ServiceListOptions) (client.ServiceListResult, error) {
	if !options.Status {
		return client.ServiceListResult{}, errors.New("service status was not requested")
	}
	return client.ServiceListResult{Items: f.services}, nil
}

func (f fakeEngine) TaskList(_ context.Context, options client.TaskListOptions) (client.TaskListResult, error) {
	if !options.Filters["desired-state"]["running"] {
		return client.TaskListResult{}, errors.New("desired running tasks were not requested")
	}
	return client.TaskListResult{Items: f.tasks}, nil
}

func (f fakeEngine) Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error) {
	return client.SystemInfoResult{Info: f.info}, nil
}

func (f fakeEngine) DaemonHost() string {
	return f.host
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
