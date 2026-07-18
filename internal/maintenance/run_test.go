package maintenance_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
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
