package docker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/api/types/swarm"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInspectorNormalizesSwarmStatus(t *testing.T) {
	t.Parallel()
	historicalAt := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)

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
					Annotations:  swarm.Annotations{Name: "database"},
					Mode:         swarm.ServiceMode{Replicated: &swarm.ReplicatedService{Replicas: uint64Pointer(1)}},
					TaskTemplate: swarm.TaskSpec{ForceUpdate: 7},
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
			{ID: "historical", Meta: swarm.Meta{UpdatedAt: historicalAt}, ServiceID: "s2", Slot: 1, NodeID: "w1", DesiredState: swarm.TaskStateShutdown, Status: swarm.TaskStatus{State: swarm.TaskStateFailed, Err: "old failure"}},
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
			{ID: "s2", Name: "database", Mode: "replicated", RunningTasks: 0, DesiredTasks: 1, Converged: false, ForceUpdate: 7},
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
		Tasks: []status.Task{
			{ID: "orphaned", Name: "agent.manager-2", ServiceID: "s3", Service: "agent", NodeID: "m2", Node: "manager-2", DesiredState: "running", State: "orphaned"},
			{ID: "healthy", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
			{ID: "failed", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "failed", Error: "exit code 1"},
			{ID: "starting", Name: "api.2", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "starting"},
			{ID: "historical", Name: "database.1", ServiceID: "s2", Service: "database", NodeID: "w1", Node: "worker-1", DesiredState: "shutdown", State: "failed", Error: "old failure", UpdatedAt: historicalAt},
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

func TestInspectorUpdatesNodeAvailability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		availability string
		want         swarm.NodeAvailability
	}{
		{name: "drain", availability: "drain", want: swarm.NodeAvailabilityDrain},
		{name: "activate", availability: "active", want: swarm.NodeAvailabilityActive},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := &nodeUpdateEngine{
				fakeEngine: fakeEngine{host: "unix:///var/run/docker.sock"},
				node: swarm.Node{
					ID:   "worker-id",
					Meta: swarm.Meta{Version: swarm.Version{Index: 7}},
					Spec: swarm.NodeSpec{
						Annotations:  swarm.Annotations{Labels: map[string]string{"storage": "shared"}},
						Role:         swarm.NodeRoleWorker,
						Availability: swarm.NodeAvailabilityPause,
					},
				},
			}
			inspector := newInspector(engine)

			err := inspector.UpdateNodeAvailability(context.Background(), "worker-id", test.availability)

			require.NoError(t, err)
			assert.Equal(t, "worker-id", engine.updatedNodeID)
			assert.Equal(t, swarm.Version{Index: 7}, engine.updateOptions.Version)
			assert.Equal(t, swarm.NodeRoleWorker, engine.updateOptions.Spec.Role)
			assert.Equal(t, map[string]string{"storage": "shared"}, engine.updateOptions.Spec.Labels)
			assert.Equal(t, test.want, engine.updateOptions.Spec.Availability)
		})
	}
}

func TestInspectorForceUpdatesService(t *testing.T) {
	t.Parallel()

	engine := &serviceUpdateEngine{
		fakeEngine: fakeEngine{host: "unix:///var/run/docker.sock"},
		service: swarm.Service{
			ID:   "service-id",
			Meta: swarm.Meta{Version: swarm.Version{Index: 9}},
			Spec: swarm.ServiceSpec{
				Annotations: swarm.Annotations{Name: "database", Labels: map[string]string{"storage": "shared"}},
				TaskTemplate: swarm.TaskSpec{
					ForceUpdate:   4,
					ContainerSpec: &swarm.ContainerSpec{Image: "postgres:17"},
				},
				Mode: swarm.ServiceMode{Replicated: &swarm.ReplicatedService{Replicas: uint64Pointer(1)}},
			},
		},
	}
	inspector := newInspector(engine)

	err := inspector.ForceUpdateService(context.Background(), "service-id")

	require.NoError(t, err)
	assert.Equal(t, "service-id", engine.updatedServiceID)
	assert.Equal(t, swarm.Version{Index: 9}, engine.updateOptions.Version)
	assert.Equal(t, "database", engine.updateOptions.Spec.Name)
	assert.Equal(t, map[string]string{"storage": "shared"}, engine.updateOptions.Spec.Labels)
	assert.Equal(t, "postgres:17", engine.updateOptions.Spec.TaskTemplate.ContainerSpec.Image)
	assert.Equal(t, uint64(5), engine.updateOptions.Spec.TaskTemplate.ForceUpdate)
	assert.Equal(t, swarm.RegistryAuthFromSpec, engine.updateOptions.RegistryAuthFrom)
}

type nodeUpdateEngine struct {
	fakeEngine
	node          swarm.Node
	updatedNodeID string
	updateOptions client.NodeUpdateOptions
}

type serviceUpdateEngine struct {
	fakeEngine
	service          swarm.Service
	updatedServiceID string
	updateOptions    client.ServiceUpdateOptions
}

func (e *serviceUpdateEngine) ServiceInspect(context.Context, string, client.ServiceInspectOptions) (client.ServiceInspectResult, error) {
	return client.ServiceInspectResult{Service: e.service}, nil
}

func (e *serviceUpdateEngine) ServiceUpdate(_ context.Context, serviceID string, options client.ServiceUpdateOptions) (client.ServiceUpdateResult, error) {
	e.updatedServiceID = serviceID
	e.updateOptions = options
	return client.ServiceUpdateResult{}, nil
}

func (e *nodeUpdateEngine) NodeInspect(context.Context, string, client.NodeInspectOptions) (client.NodeInspectResult, error) {
	return client.NodeInspectResult{Node: e.node}, nil
}

func (e *nodeUpdateEngine) NodeUpdate(_ context.Context, nodeID string, options client.NodeUpdateOptions) (client.NodeUpdateResult, error) {
	e.updatedNodeID = nodeID
	e.updateOptions = options
	return client.NodeUpdateResult{}, nil
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
	if len(options.Filters) != 0 {
		return client.TaskListResult{}, errors.New("all tasks were not requested")
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
