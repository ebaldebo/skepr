package maintenance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBeginDrainsNodeAndWaitsForMaintenanceReadiness(t *testing.T) {
	initial := healthyBeginInventory()
	evacuated := healthyBeginInventory()
	evacuated.Nodes[1].Availability = "drain"
	evacuated.DesiredTasks = nil
	evacuated.Services[0].RunningTasks = 0
	evacuated.Services[0].Converged = false
	converged := evacuated
	converged.Services = append([]status.Service(nil), evacuated.Services...)
	converged.Services[0].RunningTasks = 1
	converged.Services[0].Converged = true

	client := &beginClient{inventories: []status.Result{initial, initial, evacuated, converged}}
	store := operations.NewStore(t.TempDir())
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	beginner := maintenance.Beginner{
		Client: client,
		Store:  store,
		Now:    func() time.Time { return now },
		NewOperationID: func() (string, error) {
			return "operation-1", nil
		},
		PollInterval: time.Nanosecond,
	}

	operation, err := beginner.Begin(context.Background(), "worker-1")
	require.NoError(t, err)
	assert.Equal(t, maintenance.PhaseMaintenanceReady, operation.Phase)
	assert.True(t, operation.MutationOccurred)
	assert.Empty(t, operation.LastError)
	assert.Equal(t, []availabilityUpdate{{nodeID: "worker-id", availability: "drain"}}, client.updates)
	assert.Equal(t, 4, client.inspectCalls)
	assert.Equal(t, []maintenance.Phase{
		maintenance.PhaseCreated,
		maintenance.PhasePreflightPassed,
		maintenance.PhaseDraining,
		maintenance.PhaseEvacuating,
		maintenance.PhaseWaitingServices,
		maintenance.PhaseMaintenanceReady,
	}, phaseKeys(operation.PhaseTimestamps))

	stored, err := store.Load(operation.ID)
	require.NoError(t, err)
	assert.Equal(t, operation, stored)
}

type beginClient struct {
	inventories  []status.Result
	inspectCalls int
	updates      []availabilityUpdate
}

type availabilityUpdate struct {
	nodeID       string
	availability string
}

func (c *beginClient) Inspect(context.Context) (status.Result, error) {
	if c.inspectCalls >= len(c.inventories) {
		return status.Result{}, fmt.Errorf("unexpected inspect call %d", c.inspectCalls+1)
	}
	result := c.inventories[c.inspectCalls]
	c.inspectCalls++
	return result, nil
}

func (c *beginClient) UpdateNodeAvailability(_ context.Context, nodeID, availability string) error {
	c.updates = append(c.updates, availabilityUpdate{nodeID: nodeID, availability: availability})
	return nil
}

func healthyBeginInventory() status.Result {
	return status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "ssh://manager-1",
		Cluster: status.Cluster{
			ID:               "cluster-1",
			LocalState:       "active",
			ControlAvailable: true,
		},
		Leader: "manager-1",
		Nodes: []status.Node{
			{ID: "manager-id", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		},
		Services: []status.Service{
			{ID: "service-1", Name: "database", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Converged: true},
		},
		DesiredTasks: []status.Task{
			{ID: "task-1", Name: "database.1", ServiceID: "service-1", Service: "database", NodeID: "worker-id", Node: "worker-1", DesiredState: "running", State: "running"},
		},
	}
}

func phaseKeys(timestamps map[maintenance.Phase]time.Time) []maintenance.Phase {
	ordered := []maintenance.Phase{
		maintenance.PhaseCreated,
		maintenance.PhasePreflightPassed,
		maintenance.PhaseDraining,
		maintenance.PhaseEvacuating,
		maintenance.PhaseWaitingServices,
		maintenance.PhaseMaintenanceReady,
		maintenance.PhaseReconciling,
		maintenance.PhaseVerifyingReturn,
		maintenance.PhaseActivating,
		maintenance.PhaseVerifyingCluster,
		maintenance.PhaseCompleted,
	}
	var phases []maintenance.Phase
	for _, phase := range ordered {
		if _, exists := timestamps[phase]; exists {
			phases = append(phases, phase)
		}
	}
	return phases
}
