package drain

import (
	"context"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDrainerWaitsForEvacuationAndServiceConvergence(t *testing.T) {
	t.Parallel()

	initial := nodeDrainTestInventory()
	evacuated := nodeDrainTestInventory()
	evacuated.Nodes[1].Availability = "drain"
	evacuated.DesiredTasks = nil
	evacuated.Tasks = nil
	evacuated.Services[0].RunningTasks = 0
	evacuated.Services[0].Converged = false
	converged := nodeDrainTestInventory()
	converged.Nodes[1].Availability = "drain"
	converged.DesiredTasks = nil
	converged.Tasks = nil
	client := &drainClient{inventories: []status.Result{initial, initial, evacuated, evacuated, converged}}
	guard := &drainGuard{}
	var phases []string

	result, err := (Drainer{
		Client:       client,
		Guard:        guard,
		Timeout:      time.Second,
		PollInterval: time.Nanosecond,
		Progress: func(result Result) {
			phases = append(phases, result.Phase)
		},
	}).Drain(context.Background(), "worker-1")

	require.NoError(t, err)
	assert.Equal(t, PhaseDrained, result.Phase)
	assert.True(t, result.Evacuated)
	assert.True(t, result.ServicesConverged)
	assert.Equal(t, "drain", result.Availability)
	assert.Equal(t, []availabilityUpdate{{nodeID: "w1", availability: "drain"}}, client.updates)
	assert.Equal(t, 5, client.inspectCalls)
	assert.Equal(t, []string{PhaseDraining, PhaseEvacuating, PhaseWaitingServices, PhaseDrained}, phases)
	assert.True(t, guard.acquired)
	assert.True(t, guard.checked)
	assert.True(t, guard.released)
}

func TestDrainerRevalidatesBeforeMutation(t *testing.T) {
	t.Parallel()

	initial := nodeDrainTestInventory()
	blocked := nodeDrainTestInventory()
	blocked.Cluster.ControlAvailable = false
	client := &drainClient{inventories: []status.Result{initial, blocked}}
	guard := &drainGuard{}

	_, err := (Drainer{Client: client, Guard: guard}).Drain(context.Background(), "worker-1")

	var safetyError *SafetyError
	require.ErrorAs(t, err, &safetyError)
	assert.False(t, safetyError.Preview.SafeToDrain)
	assert.Empty(t, client.updates)
	assert.True(t, guard.released)
}

func TestDrainerBlocksActiveMaintenanceOperation(t *testing.T) {
	t.Parallel()

	client := &drainClient{inventories: []status.Result{nodeDrainTestInventory()}}
	guard := &drainGuard{ensureErr: assert.AnError}

	_, err := (Drainer{Client: client, Guard: guard}).Drain(context.Background(), "worker-1")

	var validationError *ValidationError
	require.ErrorAs(t, err, &validationError)
	assert.Equal(t, assert.AnError.Error(), validationError.Error())
	assert.Equal(t, 1, client.inspectCalls)
	assert.Empty(t, client.updates)
	assert.True(t, guard.released)
}

func TestDrainerTimeoutAfterMutationNeverActivatesNode(t *testing.T) {
	t.Parallel()

	initial := nodeDrainTestInventory()
	stillEvacuating := nodeDrainTestInventory()
	stillEvacuating.Nodes[1].Availability = "drain"
	client := &drainClient{inventories: []status.Result{initial, initial, stillEvacuating}}

	_, err := (Drainer{
		Client:       client,
		Guard:        &drainGuard{},
		Timeout:      2 * time.Millisecond,
		PollInterval: time.Microsecond,
	}).Drain(context.Background(), "worker-1")

	var mutationError *MutationError
	require.ErrorAs(t, err, &mutationError)
	assert.Equal(t, PhaseEvacuating, mutationError.Result.Phase)
	assert.ErrorIs(t, mutationError, context.DeadlineExceeded)
	assert.Equal(t, []availabilityUpdate{{nodeID: "w1", availability: "drain"}}, client.updates)
}

type drainClient struct {
	inventories  []status.Result
	inspectCalls int
	updates      []availabilityUpdate
}

type availabilityUpdate struct {
	nodeID       string
	availability string
}

type drainGuard struct {
	acquired  bool
	checked   bool
	released  bool
	ensureErr error
}

func (g *drainGuard) AcquireClusterLock(string) (func() error, error) {
	g.acquired = true
	return func() error {
		g.released = true
		return nil
	}, nil
}

func (g *drainGuard) EnsureNoActiveOperation(string) error {
	g.checked = true
	return g.ensureErr
}

func (c *drainClient) Inspect(context.Context) (status.Result, error) {
	index := c.inspectCalls
	c.inspectCalls++
	if index >= len(c.inventories) {
		return c.inventories[len(c.inventories)-1], nil
	}
	return c.inventories[index], nil
}

func (c *drainClient) UpdateNodeAvailability(_ context.Context, nodeID, availability string) error {
	c.updates = append(c.updates, availabilityUpdate{nodeID: nodeID, availability: availability})
	return nil
}

func nodeDrainTestInventory() status.Result {
	task := status.Task{ID: "t1", Name: "api.1", ServiceID: "s1", Service: "api", NodeID: "w1", Node: "worker-1", DesiredState: "running", State: "running"}
	return status.Result{
		Endpoint: "unix:///var/run/docker.sock",
		Cluster:  status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
			{ID: "w2", Hostname: "worker-2", Role: "worker", State: "ready", Availability: "active"},
		},
		Services:     []status.Service{{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Converged: true}},
		DesiredTasks: []status.Task{task},
		Tasks:        []status.Task{task},
	}
}
