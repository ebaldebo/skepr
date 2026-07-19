package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceBeginReachesMaintenanceReady(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	initial := healthyMaintenanceInventory()
	evacuated := healthyMaintenanceInventory()
	evacuated.Nodes[1].Availability = "drain"
	evacuated.DesiredTasks = nil
	evacuated.Services[0].RunningTasks = 0
	evacuated.Services[0].Converged = false
	converged := evacuated
	converged.Services = append([]status.Service(nil), evacuated.Services...)
	converged.Services[0].RunningTasks = 1
	converged.Services[0].Converged = true
	connection := &maintenanceConnection{inventories: []status.Result{initial, initial, evacuated, converged}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSuccess, exitCode)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	operation, err := store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	require.NotNil(t, operation)
	assert.Equal(t, maintenance.PhaseMaintenanceReady, operation.Phase)
	assert.Equal(t, fmt.Sprintf("Operation: %s\nTarget: worker-1 (worker-id)\nPhase: maintenance-ready\n", operation.ID), stdout.String())
	assert.Equal(t, fmt.Sprintf(`operation %s: created
operation %s: preflight-passed
operation %s: draining
operation %s: evacuating
operation %s: waiting-services
operation %s: maintenance-ready
`, operation.ID, operation.ID, operation.ID, operation.ID, operation.ID, operation.ID), stderr.String())
	assert.Equal(t, []maintenanceAvailabilityUpdate{{nodeID: "worker-id", availability: "drain"}}, connection.updates)
}

func TestMaintenanceBeginTimeoutLeavesOperationDrained(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	initial := healthyMaintenanceInventory()
	stillEvacuating := healthyMaintenanceInventory()
	stillEvacuating.Nodes[1].Availability = "drain"
	connection := &maintenanceConnection{inventories: []status.Result{initial, initial, stillEvacuating}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1", "--timeout", "2ms"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitPartialMutation, exitCode)
	assert.Empty(t, stdout.String())
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	operation, err := store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	require.NotNil(t, operation)
	assert.Equal(t, maintenance.PhaseEvacuating, operation.Phase)
	assert.True(t, operation.MutationOccurred)
	assert.Equal(t, "wait for target evacuation: context deadline exceeded", operation.LastError)
	assert.Equal(t, []maintenanceAvailabilityUpdate{{nodeID: "worker-id", availability: "drain"}}, connection.updates)
	assert.Contains(t, stderr.String(), fmt.Sprintf("maintenance begin operation %s failed in phase evacuating: wait for target evacuation: context deadline exceeded\n", operation.ID))
	assert.Contains(t, stderr.String(), fmt.Sprintf("RECOVERY: node remains drained; inspect operation %s before further mutation\n", operation.ID))
}

func TestMaintenanceBeginPersistsAffectedServiceTimeout(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	initial := healthyMaintenanceInventory()
	waitingServices := healthyMaintenanceInventory()
	waitingServices.Nodes[1].Availability = "drain"
	waitingServices.DesiredTasks = nil
	waitingServices.Services[0].RunningTasks = 0
	waitingServices.Services[0].Converged = false
	connection := &maintenanceConnection{inventories: []status.Result{initial, initial, waitingServices, waitingServices}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1", "--timeout=2ms"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitPartialMutation, exitCode)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	operation, err := store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	require.NotNil(t, operation)
	assert.Equal(t, maintenance.PhaseWaitingServices, operation.Phase)
	assert.Equal(t, "wait for affected services: context deadline exceeded", operation.LastError)
	assert.True(t, operation.TargetWorkload.AffectedServices[0].Singleton)
	assert.Contains(t, stderr.String(), "RECOVERY: node remains drained")
}

func TestMaintenanceBeginTreatsDrainErrorAsPartialMutation(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	initial := healthyMaintenanceInventory()
	connection := &maintenanceConnection{
		inventories: []status.Result{initial, initial},
		updateErr:   fmt.Errorf("connection reset after request"),
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitPartialMutation, exitCode)
	assert.Empty(t, stdout.String())
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	operation, err := store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	require.NotNil(t, operation)
	assert.Equal(t, maintenance.PhaseDraining, operation.Phase)
	assert.True(t, operation.MutationOccurred)
	assert.Equal(t, "drain target node worker-1: connection reset after request", operation.LastError)
	assert.Contains(t, stderr.String(), "RECOVERY: node remains drained")
}

func TestMaintenanceBeginRefusesUnsafePreflight(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	unsafe := healthyMaintenanceInventory()
	unsafe.Nodes[1].State = "down"
	connection := &maintenanceConnection{inventories: []status.Result{unsafe}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Equal(t, "BLOCKER: target node worker-1 state is down, expected ready\n", stdout.String())
	assert.Empty(t, stderr.String())
	assert.Empty(t, connection.updates)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	operation, err := store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	assert.Nil(t, operation)
}

func TestMaintenanceBeginJSONReportsUnsafePreflight(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	unsafe := healthyMaintenanceInventory()
	unsafe.Nodes[1].State = "down"
	connection := &maintenanceConnection{inventories: []status.Result{unsafe}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1", "--json"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSafetyGate, exitCode)
	var result preflight.Result
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
	assert.Equal(t, status.SchemaVersion, result.SchemaVersion)
	assert.False(t, result.Safe)
	assert.Contains(t, result.Findings, preflight.Finding{
		Gate:    "target_ready",
		Level:   preflight.LevelBlocker,
		Message: "target node worker-1 state is down, expected ready",
	})
	assert.NotContains(t, stdout.String(), "\x1b")
	assert.Empty(t, stderr.String())
	assert.Empty(t, connection.updates)
}

func TestMaintenanceBeginRefusesActiveOperation(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	require.NoError(t, store.Save(operations.Record{
		SchemaVersion: operations.SchemaVersion,
		ID:            "existing-operation",
		ClusterID:     "cluster-1",
		Phase:         maintenance.PhaseMaintenanceReady,
	}))
	connection := &maintenanceConnection{inventories: []status.Result{healthyMaintenanceInventory()}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSafetyGate, exitCode)
	assert.Empty(t, stdout.String())
	assert.Equal(t, "maintenance operation existing-operation is already active for cluster cluster-1 in phase maintenance-ready\n", stderr.String())
	assert.Empty(t, connection.updates)
	assert.Equal(t, 1, connection.inspectCalls)
}

func TestMaintenanceBeginJSONOutput(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	initial := healthyMaintenanceInventory()
	evacuated := healthyMaintenanceInventory()
	evacuated.Nodes[1].Availability = "drain"
	evacuated.DesiredTasks = nil
	converged := evacuated
	connection := &maintenanceConnection{inventories: []status.Result{initial, initial, evacuated, converged}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1", "--json"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSuccess, exitCode)
	var result struct {
		SchemaVersion int                   `json:"schema_version"`
		Operation     maintenance.Operation `json:"operation"`
	}
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
	assert.Equal(t, maintenance.OperationSchemaVersion, result.SchemaVersion)
	assert.NotEmpty(t, result.Operation.ID)
	assert.Equal(t, "worker-1", result.Operation.Target.Hostname)
	assert.Equal(t, maintenance.PhaseMaintenanceReady, result.Operation.Phase)
	assert.True(t, result.Operation.MutationOccurred)
	assert.NotContains(t, stdout.String(), "\x1b")
	assert.Contains(t, stderr.String(), "maintenance-ready")
}

func TestMaintenanceShowReportsDurableAndLiveState(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	require.NoError(t, store.Save(operations.Record{
		SchemaVersion:    operations.SchemaVersion,
		ID:               "operation-1",
		ClusterID:        "cluster-1",
		Endpoint:         "ssh://manager-1",
		Target:           status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		TargetWorkload:   maintenanceWorkloadSnapshot(),
		Phase:            maintenance.PhaseWaitingServices,
		MutationOccurred: true,
		LastError:        "wait for affected services: context deadline exceeded",
	}))
	live := healthyMaintenanceInventory()
	live.Nodes[1].Availability = "drain"
	live.DesiredTasks = nil
	live.Services[0].RunningTasks = 0
	live.Services[0].Converged = false
	connection := &maintenanceConnection{inventories: []status.Result{live}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "show", "operation-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Empty(t, stderr.String())
	assert.Equal(t, `Operation: operation-1
Phase: waiting-services
Cluster: cluster-1
Target: worker-1 (worker-id) worker
Mutation occurred: yes
Last error: wait for affected services: context deadline exceeded
Live target: ready drain
Live target tasks: 0 desired-running

Affected services:
  SERVICE   SERVICE ID  CLASS      RUNNING/DESIRED  STATE
  database  service-1   singleton  0/1              unconverged
`, stdout.String())
}

func TestMaintenanceShowJSONUsesLatestActiveOperation(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	baseTime := time.Date(2026, time.July, 18, 14, 0, 0, 0, time.UTC)
	for _, record := range []operations.Record{
		{SchemaVersion: operations.SchemaVersion, ID: "older-active", ClusterID: "cluster-2", Phase: maintenance.PhaseEvacuating, UpdatedAt: baseTime},
		{SchemaVersion: operations.SchemaVersion, ID: "latest-active", ClusterID: "cluster-1", Target: status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker"}, Phase: maintenance.PhaseMaintenanceReady, UpdatedAt: baseTime.Add(time.Minute)},
		{SchemaVersion: operations.SchemaVersion, ID: "newer-completed", ClusterID: "cluster-1", Phase: maintenance.PhaseCompleted, UpdatedAt: baseTime.Add(2 * time.Minute)},
	} {
		require.NoError(t, store.Save(record))
	}
	connection := &maintenanceConnection{inventories: []status.Result{healthyMaintenanceInventory()}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "show", "--json"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Empty(t, stderr.String())
	var result maintenance.ShowResult
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
	assert.Equal(t, maintenance.OperationSchemaVersion, result.SchemaVersion)
	assert.Equal(t, "latest-active", result.Operation.ID)
	require.NotNil(t, result.Live)
	assert.True(t, result.Live.ClusterMatchesOperation)
	assert.NotContains(t, stdout.String(), "\x1b")
}

func TestMaintenanceShowStillReportsRecordWhenDockerIsUnavailable(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	require.NoError(t, store.Save(operations.Record{
		SchemaVersion: operations.SchemaVersion,
		ID:            "operation-1",
		ClusterID:     "cluster-1",
		Target:        status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker"},
		Phase:         maintenance.PhaseEvacuating,
	}))
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "show", "operation-1"}, maintenanceConnectorError{}, &stdout, &stderr)

	assert.Equal(t, ExitDockerConnection, exitCode)
	assert.Empty(t, stderr.String())
	assert.Contains(t, stdout.String(), "Operation: operation-1\n")
	assert.Contains(t, stdout.String(), "Live state: unavailable: manager endpoint is unreachable\n")
}

func TestMaintenanceReconcileRestartsAffectedSingleton(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	recordedAt := time.Date(2026, time.July, 18, 15, 0, 0, 0, time.UTC)
	require.NoError(t, store.Save(operations.Record{
		SchemaVersion:    operations.SchemaVersion,
		ID:               "operation-1",
		ClusterID:        "cluster-1",
		Target:           status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker"},
		TargetWorkload:   maintenanceWorkloadSnapshot(),
		Phase:            maintenance.PhaseWaitingServices,
		PhaseTimestamps:  map[maintenance.Phase]time.Time{maintenance.PhaseWaitingServices: recordedAt},
		MutationOccurred: true,
	}))
	stalled := healthyMaintenanceInventory()
	stalled.Nodes[1].Availability = "drain"
	stalled.DesiredTasks = nil
	stalled.Services[0].RunningTasks = 0
	stalled.Services[0].Converged = false
	converged := stalled
	converged.Services = append([]status.Service(nil), stalled.Services...)
	converged.Services[0].RunningTasks = 1
	converged.Services[0].Converged = true
	connection := &maintenanceConnection{inventories: []status.Result{stalled, converged}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "reconcile", "operation-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, "Operation: operation-1\nReconciliation attempts: 1\nPhase: maintenance-ready\n", stdout.String())
	assert.Equal(t, []string{"service-1"}, connection.forceUpdates)
	assert.Contains(t, stderr.String(), "operation operation-1: reconciling\n")
	assert.Contains(t, stderr.String(), "operation operation-1: maintenance-ready\n")
	persisted, err := store.Load("operation-1")
	require.NoError(t, err)
	assert.Equal(t, maintenance.PhaseMaintenanceReady, persisted.Phase)
	require.Len(t, persisted.ReconciliationAttempts, 1)
	assert.Equal(t, maintenance.ReconciliationConverged, persisted.ReconciliationAttempts[0].Result)
}

func TestMaintenanceFinishActivatesNodeAndCompletesOperation(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	recordedAt := time.Date(2026, time.July, 18, 16, 0, 0, 0, time.UTC)
	require.NoError(t, store.Save(operations.Record{
		SchemaVersion:    operations.SchemaVersion,
		ID:               "operation-1",
		ClusterID:        "cluster-1",
		Target:           status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker"},
		Phase:            maintenance.PhaseMaintenanceReady,
		PhaseTimestamps:  map[maintenance.Phase]time.Time{maintenance.PhaseMaintenanceReady: recordedAt},
		MutationOccurred: true,
	}))
	returned := healthyMaintenanceInventory()
	returned.Nodes[1].Availability = "drain"
	active := healthyMaintenanceInventory()
	connection := &maintenanceConnection{inventories: []status.Result{returned, active}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "finish", "operation-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitSuccess, exitCode)
	assert.Equal(t, "Operation: operation-1\nTarget: worker-1 (worker-id)\nPhase: completed\n", stdout.String())
	assert.Equal(t, []maintenanceAvailabilityUpdate{{nodeID: "worker-id", availability: "active"}}, connection.updates)
	assert.Contains(t, stderr.String(), "operation operation-1: activating\n")
	assert.Contains(t, stderr.String(), "operation operation-1: completed\n")
	persisted, err := store.Load("operation-1")
	require.NoError(t, err)
	assert.Equal(t, maintenance.PhaseCompleted, persisted.Phase)
}

func TestMaintenanceFinishActivationFailureRequiresRecovery(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	require.NoError(t, store.Save(operations.Record{
		SchemaVersion:    operations.SchemaVersion,
		ID:               "operation-1",
		ClusterID:        "cluster-1",
		Target:           status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker"},
		Phase:            maintenance.PhaseMaintenanceReady,
		PhaseTimestamps:  map[maintenance.Phase]time.Time{maintenance.PhaseMaintenanceReady: time.Now()},
		MutationOccurred: true,
	}))
	returned := healthyMaintenanceInventory()
	returned.Nodes[1].Availability = "drain"
	connection := &maintenanceConnection{
		inventories: []status.Result{returned},
		updateErr:   fmt.Errorf("manager lost the update response"),
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "finish", "operation-1"}, &fakeConnector{connection: connection}, &stdout, &stderr)

	assert.Equal(t, ExitPartialMutation, exitCode)
	assert.Empty(t, stdout.String())
	assert.Contains(t, stderr.String(), "RECOVERY: node activation may have occurred")
	assert.Contains(t, stderr.String(), "never redrain automatically")
	persisted, err := store.Load("operation-1")
	require.NoError(t, err)
	assert.Equal(t, maintenance.PhaseActivating, persisted.Phase)
	assert.Contains(t, persisted.LastError, "manager lost the update response")
}

type maintenanceConnection struct {
	inventories  []status.Result
	inspectCalls int
	updates      []maintenanceAvailabilityUpdate
	forceUpdates []string
	updateErr    error
}

type maintenanceConnectorError struct{}

func (maintenanceConnectorError) Connect(context.Context, string) (status.Connection, error) {
	return nil, fmt.Errorf("manager endpoint is unreachable")
}

type maintenanceAvailabilityUpdate struct {
	nodeID       string
	availability string
}

func (c *maintenanceConnection) Inspect(context.Context) (status.Result, error) {
	if c.inspectCalls >= len(c.inventories) {
		return status.Result{}, fmt.Errorf("unexpected inspect call %d", c.inspectCalls+1)
	}
	result := c.inventories[c.inspectCalls]
	c.inspectCalls++
	return result, nil
}

func (c *maintenanceConnection) UpdateNodeAvailability(_ context.Context, nodeID, availability string) error {
	c.updates = append(c.updates, maintenanceAvailabilityUpdate{nodeID: nodeID, availability: availability})
	return c.updateErr
}

func (c *maintenanceConnection) ForceUpdateService(_ context.Context, serviceID string) error {
	c.forceUpdates = append(c.forceUpdates, serviceID)
	return c.updateErr
}

func (c *maintenanceConnection) Close() error { return nil }

func healthyMaintenanceInventory() status.Result {
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

func maintenanceWorkloadSnapshot() preflight.TargetWorkload {
	return preflight.TargetWorkload{
		DesiredRunningTaskCount: 1,
		Tasks: []preflight.WorkloadTask{
			{ID: "task-1", Name: "database.1", ServiceID: "service-1", Service: "database", State: "running"},
		},
		AffectedServices: []preflight.AffectedService{
			{ID: "service-1", Name: "database", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Singleton: true},
		},
	}
}
