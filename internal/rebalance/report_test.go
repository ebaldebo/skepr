package rebalance

import (
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildReportClassifiesReplicatedServiceDistribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		inventory            status.Result
		expectedState        string
		expectedSkew         uint64
		expectedDestinations []string
		expectedReason       string
	}{
		{
			name:                 "known opportunity",
			inventory:            reportTestInventory(),
			expectedState:        StateOpportunity,
			expectedSkew:         4,
			expectedDestinations: []string{"worker-2", "worker-3"},
		},
		{
			name: "no opportunity when counts differ by one",
			inventory: func() status.Result {
				inventory := reportTestInventory()
				inventory.DesiredTasks[2].NodeID = "w2"
				inventory.DesiredTasks[2].Node = "worker-2"
				inventory.DesiredTasks[3].NodeID = "w3"
				inventory.DesiredTasks[3].Node = "worker-3"
				inventory.Tasks = inventory.DesiredTasks
				return inventory
			}(),
			expectedState:        StateNoOpportunity,
			expectedSkew:         1,
			expectedDestinations: []string{},
		},
		{
			name: "constrained when lower count nodes cannot receive a task",
			inventory: func() status.Result {
				inventory := reportTestInventory()
				inventory.DesiredTasks[3].NodeID = "w2"
				inventory.DesiredTasks[3].Node = "worker-2"
				inventory.Tasks = inventory.DesiredTasks
				inventory.Nodes[1].Availability = "pause"
				inventory.Nodes[2].Availability = "pause"
				return inventory
			}(),
			expectedState:        StateConstrained,
			expectedSkew:         2,
			expectedDestinations: []string{},
			expectedReason:       "no lower-count node passes evaluated placement checks",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			report := BuildReport(test.inventory)

			require.Len(t, report.Services, 1)
			assessment := report.Services[0]
			assert.Equal(t, test.expectedState, assessment.State)
			assert.Equal(t, test.expectedSkew, assessment.Skew)
			assert.Equal(t, test.expectedDestinations, nodeHostnames(assessment.KnownEligibleDestinations))
			assert.Equal(t, test.expectedReason, assessment.Reason)
		})
	}
}

func TestBuildReportExplainsServicesItCannotAssess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		service        status.Service
		expectedReason string
	}{
		{
			name:           "global service",
			service:        status.Service{ID: "s1", Name: "agent", Mode: "global", RunningTasks: 3, DesiredTasks: 3, Converged: true},
			expectedReason: "mode global is not replicated",
		},
		{
			name:           "unconverged service",
			service:        status.Service{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 3, DesiredTasks: 4},
			expectedReason: "service is unconverged at 3/4 running tasks",
		},
		{
			name:           "placement preferences",
			service:        status.Service{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 4, DesiredTasks: 4, Converged: true, PlacementPreferences: []string{"spread=node.labels.zone"}},
			expectedReason: "placement preferences are configured: spread=node.labels.zone",
		},
		{
			name:           "unsupported placement constraint",
			service:        status.Service{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 4, DesiredTasks: 4, Converged: true, PlacementConstraints: []string{"node.labels.region~=east"}},
			expectedReason: "placement constraints are not fully evaluated: node.labels.region~=east",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			inventory := reportTestInventory()
			inventory.Services = []status.Service{test.service}
			report := BuildReport(inventory)

			require.Len(t, report.Services, 1)
			assert.Equal(t, StateNotAssessed, report.Services[0].State)
			assert.Equal(t, test.expectedReason, report.Services[0].Reason)
			assert.Equal(t, 1, report.Summary.NotAssessedServices)
		})
	}
}

func TestBuildReportSummarizesDeclaredReservations(t *testing.T) {
	t.Parallel()

	inventory := status.Result{
		Nodes: []status.Node{
			{ID: "w2", Hostname: "worker-2", Resources: status.Resources{NanoCPUs: 2_000_000_000, MemoryBytes: 4 << 30}},
			{ID: "w1", Hostname: "worker-1", Resources: status.Resources{NanoCPUs: 4_000_000_000, MemoryBytes: 8 << 30}},
		},
		Tasks: []status.Task{
			{ID: "t1", Name: "api.1", Service: "api", NodeID: "w1", State: "running", Reservations: status.Resources{NanoCPUs: 1_000_000_000, MemoryBytes: 512 << 20}},
			{ID: "t2", Name: "api.2", Service: "api", NodeID: "w1", State: "preparing"},
			{ID: "t3", Name: "worker.1", Service: "worker", NodeID: "w1", State: "running", Reservations: status.Resources{MemoryBytes: 256 << 20}},
			{ID: "t4", Name: "old.1", Service: "old", NodeID: "w1", State: "failed"},
			{ID: "t5", Name: "pending.1", Service: "pending", State: "pending"},
		},
	}

	report := BuildReport(inventory)

	assert.Equal(t, 3, report.Summary.ActiveTasks)
	assert.Equal(t, 2, report.Summary.TasksWithoutCPUReservations)
	assert.Equal(t, 1, report.Summary.TasksWithoutMemoryReservations)
	assert.Equal(t, []NodeReservation{
		{
			ID:          "w1",
			Hostname:    "worker-1",
			ActiveTasks: 3,
			Resources: status.NodeResources{
				Capacity:  status.Resources{NanoCPUs: 4_000_000_000, MemoryBytes: 8 << 30},
				Reserved:  status.Resources{NanoCPUs: 1_000_000_000, MemoryBytes: 768 << 20},
				Available: status.Resources{NanoCPUs: 3_000_000_000, MemoryBytes: 7424 << 20},
			},
			TasksWithoutCPUReservations: []TaskReference{
				{ID: "t2", Name: "api.2", Service: "api"},
				{ID: "t3", Name: "worker.1", Service: "worker"},
			},
			TasksWithoutMemoryReservations: []TaskReference{{ID: "t2", Name: "api.2", Service: "api"}},
		},
		{
			ID:       "w2",
			Hostname: "worker-2",
			Resources: status.NodeResources{
				Capacity:  status.Resources{NanoCPUs: 2_000_000_000, MemoryBytes: 4 << 30},
				Available: status.Resources{NanoCPUs: 2_000_000_000, MemoryBytes: 4 << 30},
			},
			TasksWithoutCPUReservations:    []TaskReference{},
			TasksWithoutMemoryReservations: []TaskReference{},
		},
	}, report.NodeReservations)
}

func reportTestInventory() status.Result {
	tasks := []status.Task{
		{ID: "t1", ServiceID: "s1", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t2", ServiceID: "s1", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t3", ServiceID: "s1", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
		{ID: "t4", ServiceID: "s1", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"},
	}
	return status.Result{
		Endpoint: "unix:///var/run/docker.sock",
		Cluster:  status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "w1", Hostname: "worker-1", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", State: "ready", Availability: "active"},
			{ID: "w3", Hostname: "worker-3", State: "ready", Availability: "active"},
		},
		Services:     []status.Service{{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 4, DesiredTasks: 4, Converged: true}},
		DesiredTasks: tasks,
		Tasks:        tasks,
	}
}

func nodeHostnames(nodes []NodeTaskCount) []string {
	hostnames := make([]string, 0, len(nodes))
	for _, node := range nodes {
		hostnames = append(hostnames, node.Hostname)
	}
	return hostnames
}
