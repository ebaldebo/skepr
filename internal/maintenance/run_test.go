package maintenance_test

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionRunnerHoldsClusterLockAcrossUpdateCommand(t *testing.T) {
	store := operations.NewStore(t.TempDir())
	client := &transactionClient{availability: "active"}
	executor := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	runner := maintenance.TransactionRunner{
		Client: client, Store: store, Executor: executor, Timeout: time.Second, ReturnTimeout: time.Second, PollInterval: time.Millisecond,
	}
	result := make(chan error, 1)
	go func() {
		_, err := runner.Run(context.Background(), "worker-1", maintenance.RunCommands{Update: []string{"update"}}, []string{"manager-1"}, nil)
		result <- err
	}()

	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("update command did not start")
	}
	operation, err := store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	require.NotNil(t, operation)

	_, err = runner.Resume(context.Background(), operation.ID, "")

	var running *maintenance.OperationAlreadyRunningError
	require.ErrorAs(t, err, &running)
	assert.Equal(t, "operation already running: "+operation.ID, err.Error())
	assert.Equal(t, 1, executor.calls())
	close(executor.release)
	require.NoError(t, <-result)
}

func TestOSCommandExecutorTerminatesCommandProcessGroupOnCancellation(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "child-finished")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- (maintenance.OSCommandExecutor{}).Run(ctx, []string{os.Args[0], "-test.run=TestOSCommandProcessHelper", "--", "parent", marker}, io.Discard, io.Discard)
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(marker + ".ready")
		return err == nil
	}, time.Second, time.Millisecond)

	cancel()
	require.Error(t, <-done)
	time.Sleep(250 * time.Millisecond)
	_, err := os.Stat(marker)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestTransactionRunnerResumesAppliedReconciliationAttempt(t *testing.T) {
	store := operations.NewStore(t.TempDir())
	before := uint64(0)
	operation := maintenance.Operation{
		SchemaVersion: maintenance.OperationSchemaVersion,
		ID:            "operation-1",
		ClusterID:     "cluster-1",
		Target:        status.Node{ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "drain"},
		TargetWorkload: preflight.TargetWorkload{AffectedServices: []preflight.AffectedService{{
			ID: "service-1", Name: "database", Mode: "replicated", DesiredTasks: 1, Singleton: true,
		}}},
		Phase:            maintenance.PhaseReconciling,
		PhaseTimestamps:  map[maintenance.Phase]time.Time{maintenance.PhaseReconciling: time.Now()},
		MutationOccurred: true,
		ReconciliationAttempts: []maintenance.ReconciliationAttempt{{
			ServiceID: "service-1", Service: "database", ForceUpdateBefore: &before, StartedAt: time.Now(), Result: maintenance.ReconciliationStarted,
		}},
		Run: &maintenance.RunState{
			Phase:           maintenance.RunPhasePreCompleted,
			TargetHostname:  "worker-1",
			Commands:        maintenance.RunCommands{Update: []string{"true"}},
			PhaseTimestamps: map[maintenance.RunPhase]time.Time{maintenance.RunPhasePreCompleted: time.Now()},
		},
	}
	require.NoError(t, store.Save(operation))
	client := &reconcilingTransactionClient{availability: "drain"}

	result, err := (maintenance.TransactionRunner{
		Client: client, Store: store, Timeout: time.Second, ReturnTimeout: time.Second, PollInterval: time.Millisecond,
	}).Resume(context.Background(), operation.ID, "")

	require.NoError(t, err)
	assert.Equal(t, maintenance.PhaseCompleted, result.Phase)
	assert.Equal(t, maintenance.RunPhaseCompleted, result.Run.Phase)
	assert.Equal(t, 0, client.forceUpdates)
}

func TestOSCommandProcessHelper(t *testing.T) {
	separator := -1
	for index, argument := range os.Args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator == -1 || len(os.Args) <= separator+2 {
		return
	}
	mode := os.Args[separator+1]
	marker := os.Args[separator+2]
	if mode == "child" {
		time.Sleep(150 * time.Millisecond)
		require.NoError(t, os.WriteFile(marker, nil, 0o600))
		return
	}
	child := exec.Command(os.Args[0], "-test.run=TestOSCommandProcessHelper", "--", "child", marker)
	require.NoError(t, child.Start())
	require.NoError(t, os.WriteFile(marker+".ready", nil, 0o600))
	_ = child.Wait()
}

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	runs    int
}

func (e *blockingExecutor) Run(ctx context.Context, _ []string, _, _ io.Writer) error {
	e.mu.Lock()
	e.runs++
	e.mu.Unlock()
	e.once.Do(func() { close(e.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.release:
		return nil
	}
}

func (e *blockingExecutor) calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runs
}

type transactionClient struct {
	mu           sync.Mutex
	availability string
}

func (c *transactionClient) Inspect(context.Context) (status.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return status.Result{
		Endpoint: "ssh://manager-1",
		Cluster:  status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Leader:   "manager-1",
		Nodes: []status.Node{
			{ID: "manager-id", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: c.availability},
		},
	}, nil
}

func (c *transactionClient) UpdateNodeAvailability(_ context.Context, _ string, availability string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.availability = availability
	return nil
}

func (c *transactionClient) ForceUpdateService(context.Context, string) error { return nil }

type reconcilingTransactionClient struct {
	mu           sync.Mutex
	inspectCalls int
	availability string
	forceUpdates int
}

func (c *reconcilingTransactionClient) Inspect(context.Context) (status.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inspectCalls++
	service := status.Service{ID: "service-1", Name: "database", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Converged: true, ForceUpdate: 1}
	if c.inspectCalls == 1 {
		service.RunningTasks = 0
		service.Converged = false
	}
	return status.Result{
		Cluster: status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Leader:  "manager-1",
		Nodes: []status.Node{
			{ID: "manager-id", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: c.availability},
		},
		Services: []status.Service{service},
	}, nil
}

func (c *reconcilingTransactionClient) UpdateNodeAvailability(_ context.Context, _ string, availability string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.availability = availability
	return nil
}

func (c *reconcilingTransactionClient) ForceUpdateService(context.Context, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forceUpdates++
	return nil
}
