package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconcileRestartsOnlyAffectedStalledSingleton(t *testing.T) {
	operation := Operation{
		SchemaVersion: OperationSchemaVersion,
		ID:            "operation-1",
		ClusterID:     "cluster-1",
		Target:        status.Node{ID: "worker-id", Hostname: "worker-1"},
		TargetWorkload: preflight.TargetWorkload{AffectedServices: []preflight.AffectedService{
			{ID: "affected-id", Name: "database", Mode: "replicated", DesiredTasks: 1, Singleton: true},
		}},
		Phase:           PhaseWaitingServices,
		PhaseTimestamps: map[Phase]time.Time{PhaseWaitingServices: time.Now()},
	}
	stalled := reconciliationInventory()
	converged := reconciliationInventory()
	converged.Services[0].RunningTasks = 1
	converged.Services[0].Converged = true
	client := &reconcileClient{inventories: []status.Result{stalled, converged}}
	store := &reconcileStore{operation: operation}

	result, err := (Reconciler{
		Client:       client,
		Store:        store,
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
	}).Reconcile(context.Background(), operation.ID)

	require.NoError(t, err)
	assert.Equal(t, PhaseMaintenanceReady, result.Phase)
	assert.Equal(t, []string{"affected-id"}, client.forceUpdates)
	require.Len(t, result.ReconciliationAttempts, 1)
	assert.Equal(t, "affected-id", result.ReconciliationAttempts[0].ServiceID)
	require.NotNil(t, result.ReconciliationAttempts[0].ForceUpdateBefore)
	assert.Equal(t, uint64(0), *result.ReconciliationAttempts[0].ForceUpdateBefore)
	assert.Equal(t, ReconciliationConverged, result.ReconciliationAttempts[0].Result)
}

func TestReconcileRevalidatesEachServiceBeforeMutation(t *testing.T) {
	operation := reconciliationOperation()
	operation.TargetWorkload.AffectedServices = append(operation.TargetWorkload.AffectedServices, preflight.AffectedService{
		ID: "second-id", Name: "cache", Mode: "replicated", DesiredTasks: 1, Singleton: true,
	})
	client := &changingReconcileClient{}

	_, err := (Reconciler{
		Client:       client,
		Store:        &reconcileStore{operation: operation},
		Timeout:      5 * time.Millisecond,
		PollInterval: time.Millisecond,
	}).Reconcile(context.Background(), operation.ID)

	var safetyError *ReconcileSafetyError
	require.ErrorAs(t, err, &safetyError)
	assert.ErrorContains(t, err, "affected service cache is 1/2 in replicated mode, expected a replicated singleton at 0/1")
	assert.Equal(t, []string{"affected-id"}, client.forceUpdates)
}

func TestReconcileRejectsUnsafeLiveStateWithoutMutation(t *testing.T) {
	tests := []struct {
		name      string
		inventory status.Result
		wantError string
	}{
		{
			name: "different cluster",
			inventory: func() status.Result {
				result := reconciliationInventory()
				result.Cluster.ID = "other-cluster"
				return result
			}(),
			wantError: "does not match operation cluster",
		},
		{
			name: "target is active",
			inventory: func() status.Result {
				result := reconciliationInventory()
				result.Nodes[1].Availability = "active"
				return result
			}(),
			wantError: "expected drain",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operation := reconciliationOperation()
			client := &reconcileClient{inventories: []status.Result{test.inventory}}

			_, err := (Reconciler{Client: client, Store: &reconcileStore{operation: operation}}).Reconcile(context.Background(), operation.ID)

			var safetyError *ReconcileSafetyError
			require.ErrorAs(t, err, &safetyError)
			assert.ErrorContains(t, err, test.wantError)
			assert.Empty(t, client.forceUpdates)
		})
	}
}

func TestReconcileMutationFailurePreservesStartedAttemptForInspection(t *testing.T) {
	operation := reconciliationOperation()
	client := &reconcileClient{
		inventories: []status.Result{reconciliationInventory()},
		forceErr:    errors.New("manager lost the update response"),
	}
	store := &reconcileStore{operation: operation}

	result, err := (Reconciler{Client: client, Store: store}).Reconcile(context.Background(), operation.ID)

	var reconcileError *ReconcileError
	require.ErrorAs(t, err, &reconcileError)
	assert.Equal(t, PhaseReconciling, result.Phase)
	assert.Contains(t, result.LastError, "manager lost the update response")
	require.Len(t, result.ReconciliationAttempts, 1)
	assert.Equal(t, ReconciliationStarted, result.ReconciliationAttempts[0].Result)
	assert.Nil(t, result.ReconciliationAttempts[0].CompletedAt)
	require.NotNil(t, result.ReconciliationAttempts[0].ForceUpdateBefore)
	assert.Equal(t, uint64(0), *result.ReconciliationAttempts[0].ForceUpdateBefore)
	assert.Equal(t, result, store.operation)
}

func TestReconcileConvergenceTimeoutPreservesStartedAttemptAndResumesWithoutDuplicateMutation(t *testing.T) {
	operation := reconciliationOperation()
	client := &stalledReconcileClient{inventory: reconciliationInventory()}
	store := &reconcileStore{operation: operation}

	result, err := (Reconciler{
		Client: client, Store: store, Timeout: 5 * time.Millisecond, PollInterval: time.Millisecond,
	}).Reconcile(context.Background(), operation.ID)

	var reconcileError *ReconcileError
	require.ErrorAs(t, err, &reconcileError)
	assert.Equal(t, PhaseReconciling, result.Phase)
	require.Len(t, result.ReconciliationAttempts, 1)
	assert.Equal(t, ReconciliationStarted, result.ReconciliationAttempts[0].Result)
	assert.Nil(t, result.ReconciliationAttempts[0].CompletedAt)
	assert.Equal(t, 1, client.forceUpdates)

	client.inventory.Services[0].ForceUpdate = 1
	client.inventory.Services[0].RunningTasks = 1
	client.inventory.Services[0].Converged = true
	result, err = (Reconciler{
		Client: client, Store: store, Timeout: time.Second, PollInterval: time.Millisecond,
	}).Reconcile(context.Background(), operation.ID)

	require.NoError(t, err)
	assert.Equal(t, PhaseMaintenanceReady, result.Phase)
	assert.Equal(t, 1, client.forceUpdates)
	assert.Empty(t, result.ReconciliationAttempts[0].Error)
}

func TestReconcileResumesAppliedPersistedAttemptWithoutSecondForceUpdate(t *testing.T) {
	before := uint64(4)
	operation := reconciliationOperation()
	operation.Phase = PhaseReconciling
	operation.PhaseTimestamps[PhaseReconciling] = time.Now()
	operation.ReconciliationAttempts = []ReconciliationAttempt{{
		ServiceID: "affected-id", Service: "database", StartedAt: time.Now(), Result: ReconciliationStarted, ForceUpdateBefore: &before,
	}}
	applied := reconciliationInventory()
	applied.Services[0].ForceUpdate = 5
	converged := applied
	converged.Services = append([]status.Service(nil), applied.Services...)
	converged.Services[0].RunningTasks = 1
	converged.Services[0].Converged = true
	client := &reconcileClient{inventories: []status.Result{applied, converged}}

	result, err := (Reconciler{
		Client: client, Store: &reconcileStore{operation: operation}, Timeout: time.Second, PollInterval: time.Millisecond,
	}).Reconcile(context.Background(), operation.ID)

	require.NoError(t, err)
	assert.Equal(t, PhaseMaintenanceReady, result.Phase)
	assert.Empty(t, client.forceUpdates)
	require.Len(t, result.ReconciliationAttempts, 1)
	assert.Equal(t, ReconciliationConverged, result.ReconciliationAttempts[0].Result)
}

func TestReconcileRetriesPersistedAttemptOnlyAfterCounterProvesNotApplied(t *testing.T) {
	before := uint64(4)
	operation := reconciliationOperation()
	operation.Phase = PhaseReconciling
	operation.PhaseTimestamps[PhaseReconciling] = time.Now()
	operation.ReconciliationAttempts = []ReconciliationAttempt{{
		ServiceID: "affected-id", Service: "database", StartedAt: time.Now(), Result: ReconciliationStarted, ForceUpdateBefore: &before,
	}}
	unchanged := reconciliationInventory()
	unchanged.Services[0].ForceUpdate = 4
	converged := unchanged
	converged.Services = append([]status.Service(nil), unchanged.Services...)
	converged.Services[0].ForceUpdate = 5
	converged.Services[0].RunningTasks = 1
	converged.Services[0].Converged = true
	client := &reconcileClient{inventories: []status.Result{unchanged, converged}}

	result, err := (Reconciler{
		Client: client, Store: &reconcileStore{operation: operation}, Timeout: time.Second, PollInterval: time.Millisecond,
	}).Reconcile(context.Background(), operation.ID)

	require.NoError(t, err)
	assert.Equal(t, PhaseMaintenanceReady, result.Phase)
	assert.Equal(t, []string{"affected-id"}, client.forceUpdates)
	require.Len(t, result.ReconciliationAttempts, 1)
	assert.Equal(t, ReconciliationConverged, result.ReconciliationAttempts[0].Result)
}

type reconcileClient struct {
	inventories  []status.Result
	inspectCalls int
	forceUpdates []string
	forceErr     error
}

type changingReconcileClient struct {
	forceUpdates []string
}

type stalledReconcileClient struct {
	inventory    status.Result
	forceUpdates int
}

func (c *stalledReconcileClient) Inspect(context.Context) (status.Result, error) {
	return c.inventory, nil
}

func (c *stalledReconcileClient) ForceUpdateService(context.Context, string) error {
	c.forceUpdates++
	return nil
}

func (c *changingReconcileClient) Inspect(context.Context) (status.Result, error) {
	result := reconciliationInventory()
	result.Services = append(result.Services, status.Service{
		ID: "second-id", Name: "cache", Mode: "replicated", RunningTasks: 0, DesiredTasks: 1, Converged: false,
	})
	if len(c.forceUpdates) > 0 {
		result.Services[0].RunningTasks = 1
		result.Services[0].Converged = true
		result.Services[2].RunningTasks = 1
		result.Services[2].DesiredTasks = 2
	}
	return result, nil
}

func (c *changingReconcileClient) ForceUpdateService(_ context.Context, serviceID string) error {
	c.forceUpdates = append(c.forceUpdates, serviceID)
	return nil
}

func (c *reconcileClient) Inspect(context.Context) (status.Result, error) {
	result := c.inventories[c.inspectCalls]
	c.inspectCalls++
	return result, nil
}

func (c *reconcileClient) ForceUpdateService(_ context.Context, serviceID string) error {
	c.forceUpdates = append(c.forceUpdates, serviceID)
	return c.forceErr
}

type reconcileStore struct {
	operation Operation
}

func (s *reconcileStore) Load(string) (Operation, error) {
	return s.operation, nil
}

func (s *reconcileStore) Save(operation Operation) error {
	s.operation = operation
	return nil
}

func (s *reconcileStore) AcquireClusterLock(string) (ClusterLock, error) {
	return reconcileLock{}, nil
}

type reconcileLock struct{}

func (reconcileLock) Release() error { return nil }

func reconciliationInventory() status.Result {
	return status.Result{
		Cluster: status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "manager-id", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "drain"},
		},
		Services: []status.Service{
			{ID: "affected-id", Name: "database", Mode: "replicated", RunningTasks: 0, DesiredTasks: 1, Converged: false},
			{ID: "unrelated-id", Name: "queue", Mode: "replicated", RunningTasks: 0, DesiredTasks: 1, Converged: false},
		},
	}
}

func reconciliationOperation() Operation {
	return Operation{
		SchemaVersion: OperationSchemaVersion,
		ID:            "operation-1",
		ClusterID:     "cluster-1",
		Target:        status.Node{ID: "worker-id", Hostname: "worker-1"},
		TargetWorkload: preflight.TargetWorkload{AffectedServices: []preflight.AffectedService{
			{ID: "affected-id", Name: "database", Mode: "replicated", DesiredTasks: 1, Singleton: true},
		}},
		Phase:           PhaseWaitingServices,
		PhaseTimestamps: map[Phase]time.Time{PhaseWaitingServices: time.Now()},
	}
}
