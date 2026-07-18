package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFinishActivatesReturnedNodeAndCompletesOperation(t *testing.T) {
	operation := finishOperation()
	returned := finishInventory("drain")
	active := finishInventory("active")
	client := &finishClient{inventories: []status.Result{returned, active}}
	store := &finishStore{operation: operation}

	result, err := (Finisher{
		Client:       client,
		Store:        store,
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
	}).Finish(context.Background(), operation.ID)

	require.NoError(t, err)
	assert.Equal(t, PhaseCompleted, result.Phase)
	assert.Equal(t, []finishAvailabilityUpdate{{nodeID: "worker-id", availability: "active"}}, client.updates)
	assert.Equal(t, result, store.operation)
	assert.Contains(t, result.PhaseTimestamps, PhaseVerifyingReturn)
	assert.Contains(t, result.PhaseTimestamps, PhaseActivating)
	assert.Contains(t, result.PhaseTimestamps, PhaseVerifyingCluster)
	assert.Contains(t, result.PhaseTimestamps, PhaseCompleted)
}

func TestFinishRejectsUnsafeReturnWithoutActivation(t *testing.T) {
	tests := []struct {
		name      string
		inventory status.Result
		wantError string
	}{
		{
			name: "different cluster",
			inventory: func() status.Result {
				result := finishInventory("drain")
				result.Cluster.ID = "other-cluster"
				return result
			}(),
			wantError: "does not match operation cluster",
		},
		{
			name:      "target already active",
			inventory: finishInventory("active"),
			wantError: "expected drain",
		},
		{
			name: "unhealthy manager",
			inventory: func() status.Result {
				result := finishInventory("drain")
				result.Nodes[1].ManagerStatus = "unreachable"
				return result
			}(),
			wantError: "manager-2 status is unreachable",
		},
		{
			name: "manager quorum",
			inventory: func() status.Result {
				result := finishInventory("drain")
				result.Nodes = append(result.Nodes[:2], result.Nodes[3:]...)
				result.Nodes[1].State = "down"
				return result
			}(),
			wantError: "1 healthy managers do not meet quorum requirement 2 of 2",
		},
		{
			name: "unconverged service",
			inventory: func() status.Result {
				result := finishInventory("drain")
				result.Services[0].RunningTasks = 1
				result.Services[0].Converged = false
				return result
			}(),
			wantError: "service api has 1/2 running tasks",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operation := finishOperation()
			client := &finishClient{inventories: []status.Result{test.inventory}}

			_, err := (Finisher{Client: client, Store: &finishStore{operation: operation}}).Finish(context.Background(), operation.ID)

			var safetyError *FinishSafetyError
			require.ErrorAs(t, err, &safetyError)
			assert.ErrorContains(t, err, test.wantError)
			assert.Empty(t, client.updates)
		})
	}
}

func TestFinishActivationFailureIsDurableAndNeverRedrains(t *testing.T) {
	operation := finishOperation()
	client := &finishClient{
		inventories: []status.Result{finishInventory("drain")},
		updateErr:   errors.New("manager lost the update response"),
	}
	store := &finishStore{operation: operation}

	result, err := (Finisher{Client: client, Store: store}).Finish(context.Background(), operation.ID)

	var finishError *FinishError
	require.ErrorAs(t, err, &finishError)
	assert.Equal(t, PhaseActivating, result.Phase)
	assert.Contains(t, result.LastError, "manager lost the update response")
	assert.Equal(t, []finishAvailabilityUpdate{{nodeID: "worker-id", availability: "active"}}, client.updates)
	assert.Equal(t, result, store.operation)
}

func TestFinishVerificationTimeoutNeverRedrains(t *testing.T) {
	operation := finishOperation()
	stillDrained := finishInventory("drain")
	client := &finishClient{inventories: []status.Result{stillDrained, stillDrained}}
	store := &finishStore{operation: operation}

	result, err := (Finisher{
		Client:       client,
		Store:        store,
		Timeout:      5 * time.Millisecond,
		PollInterval: time.Millisecond,
	}).Finish(context.Background(), operation.ID)

	var finishError *FinishError
	require.ErrorAs(t, err, &finishError)
	assert.Equal(t, PhaseVerifyingCluster, result.Phase)
	assert.ErrorContains(t, err, "context deadline exceeded")
	assert.Equal(t, []finishAvailabilityUpdate{{nodeID: "worker-id", availability: "active"}}, client.updates)
	assert.Equal(t, result, store.operation)
}

func TestFinishResumesActivationIntentWithoutRedrainingOrReactivating(t *testing.T) {
	operation := finishOperation()
	operation.Phase = PhaseActivating
	operation.PhaseTimestamps[PhaseActivating] = time.Now()
	client := &finishClient{inventories: []status.Result{finishInventory("active"), finishInventory("active")}}
	store := &finishStore{operation: operation}

	result, err := (Finisher{Client: client, Store: store}).Finish(context.Background(), operation.ID)

	require.NoError(t, err)
	assert.Equal(t, PhaseCompleted, result.Phase)
	assert.Empty(t, client.updates)
}

type finishClient struct {
	inventories  []status.Result
	inspectCalls int
	updates      []finishAvailabilityUpdate
	updateErr    error
}

type finishAvailabilityUpdate struct {
	nodeID       string
	availability string
}

func (c *finishClient) Inspect(context.Context) (status.Result, error) {
	index := c.inspectCalls
	if index >= len(c.inventories) {
		index = len(c.inventories) - 1
	}
	result := c.inventories[index]
	c.inspectCalls++
	return result, nil
}

func (c *finishClient) UpdateNodeAvailability(_ context.Context, nodeID, availability string) error {
	c.updates = append(c.updates, finishAvailabilityUpdate{nodeID: nodeID, availability: availability})
	return c.updateErr
}

type finishStore struct {
	operation Operation
}

func (s *finishStore) Load(string) (Operation, error) {
	return s.operation, nil
}

func (s *finishStore) Save(operation Operation) error {
	s.operation = operation
	return nil
}

func (s *finishStore) AcquireClusterLock(string) (ClusterLock, error) {
	return finishLock{}, nil
}

type finishLock struct{}

func (finishLock) Release() error { return nil }

func finishOperation() Operation {
	recordedAt := time.Date(2026, time.July, 18, 16, 0, 0, 0, time.UTC)
	return Operation{
		SchemaVersion:    OperationSchemaVersion,
		ID:               "operation-1",
		ClusterID:        "cluster-1",
		Target:           status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker"},
		Phase:            PhaseMaintenanceReady,
		PhaseTimestamps:  map[Phase]time.Time{PhaseMaintenanceReady: recordedAt},
		MutationOccurred: true,
	}
}

func finishInventory(targetAvailability string) status.Result {
	return status.Result{
		Cluster: status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "manager-1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "manager-2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			{ID: "manager-3", Hostname: "manager-3", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			{ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: targetAvailability},
		},
		Services: []status.Service{
			{ID: "service-1", Name: "api", Mode: "replicated", RunningTasks: 2, DesiredTasks: 2, Converged: true},
		},
	}
}
