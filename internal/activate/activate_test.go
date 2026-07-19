package activate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSafetyReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		inventory       status.Result
		requestedNode   string
		expectedSafe    bool
		expectedBlocker string
	}{
		{
			name:          "drained worker is safe",
			inventory:     healthyDrainedInventory(),
			requestedNode: "worker-1",
			expectedSafe:  true,
		},
		{
			name: "drained manager is counted healthy",
			inventory: func() status.Result {
				inventory := healthyDrainedInventory()
				inventory.Nodes[0].Availability = "drain"
				return inventory
			}(),
			requestedNode: "manager-1",
			expectedSafe:  true,
		},
		{
			name: "unhealthy manager blocks",
			inventory: func() status.Result {
				inventory := healthyDrainedInventory()
				inventory.Nodes[0].State = "down"
				return inventory
			}(),
			requestedNode:   "worker-1",
			expectedBlocker: "Swarm manager manager-1 is unhealthy: state is down, expected ready",
		},
		{
			name: "unconverged service blocks",
			inventory: func() status.Result {
				inventory := healthyDrainedInventory()
				inventory.Services[0].RunningTasks = 0
				inventory.Services[0].Converged = false
				return inventory
			}(),
			requestedNode:   "worker-1",
			expectedBlocker: "Swarm service api has 0 of 1 running tasks",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			report := BuildSafetyReport(test.inventory, test.requestedNode, "drain")

			assert.Equal(t, test.expectedSafe, report.Safe)
			if test.expectedBlocker == "" {
				assert.Empty(t, findingMessages(report.Findings, preflight.LevelBlocker))
				return
			}
			assert.Contains(t, findingMessages(report.Findings, preflight.LevelBlocker), test.expectedBlocker)
		})
	}
}

func TestActivateWaitsForActiveTargetAndConvergedServices(t *testing.T) {
	t.Parallel()

	drained := healthyDrainedInventory()
	transitioning := healthyDrainedInventory()
	transitioning.Nodes[1].Availability = "active"
	transitioning.Services[0].RunningTasks = 0
	transitioning.Services[0].Converged = false
	active := healthyDrainedInventory()
	active.Nodes[1].Availability = "active"
	client := &fakeClient{inventories: []status.Result{drained, drained, transitioning, active}}
	guard := &fakeGuard{}
	activator := Activator{Client: client, Guard: guard, Timeout: time.Second, PollInterval: time.Nanosecond}

	result, err := activator.Activate(context.Background(), "worker-1")

	require.NoError(t, err)
	assert.Equal(t, PhaseActive, result.Phase)
	assert.Equal(t, "active", result.Availability)
	assert.Equal(t, 1, result.ConvergedServices)
	assert.Equal(t, []availabilityUpdate{{nodeID: "w1", availability: "active"}}, client.updates)
	assert.Equal(t, 4, client.inspectCalls)
	assert.Equal(t, 1, guard.acquireCalls)
	assert.Equal(t, 1, guard.ensureCalls)
	assert.True(t, guard.released)
}

func TestActivateRevalidatesBeforeMutation(t *testing.T) {
	t.Parallel()

	initial := healthyDrainedInventory()
	blocked := healthyDrainedInventory()
	blocked.Cluster.ControlAvailable = false
	client := &fakeClient{inventories: []status.Result{initial, blocked}}
	guard := &fakeGuard{}

	_, err := (Activator{Client: client, Guard: guard}).Activate(context.Background(), "worker-1")

	var safetyError *SafetyError
	require.ErrorAs(t, err, &safetyError)
	assert.False(t, safetyError.Report.Safe)
	assert.Empty(t, client.updates)
	assert.True(t, guard.released)
}

func TestActivateBlocksActiveMaintenanceOperation(t *testing.T) {
	t.Parallel()

	client := &fakeClient{inventories: []status.Result{healthyDrainedInventory()}}
	guard := &fakeGuard{ensureErr: assert.AnError}

	_, err := (Activator{Client: client, Guard: guard}).Activate(context.Background(), "worker-1")

	var validationError *ValidationError
	require.ErrorAs(t, err, &validationError)
	assert.Equal(t, assert.AnError.Error(), validationError.Error())
	assert.Equal(t, 1, client.inspectCalls)
	assert.Empty(t, client.updates)
	assert.True(t, guard.released)
}

func TestActivateTimeoutAfterRequestNeverRedrainsTarget(t *testing.T) {
	t.Parallel()

	drained := healthyDrainedInventory()
	client := &fakeClient{inspect: func(call int) (status.Result, error) {
		return drained, nil
	}}
	activator := Activator{Client: client, Guard: &fakeGuard{}, Timeout: time.Millisecond, PollInterval: time.Nanosecond}

	result, err := activator.Activate(context.Background(), "worker-1")

	var mutationError *MutationError
	require.ErrorAs(t, err, &mutationError)
	assert.Equal(t, PhaseVerifying, result.Phase)
	assert.Equal(t, []availabilityUpdate{{nodeID: "w1", availability: "active"}}, client.updates)
}

type fakeClient struct {
	inventories  []status.Result
	inspect      func(int) (status.Result, error)
	inspectCalls int
	updates      []availabilityUpdate
}

type availabilityUpdate struct {
	nodeID       string
	availability string
}

func (c *fakeClient) Inspect(context.Context) (status.Result, error) {
	call := c.inspectCalls
	c.inspectCalls++
	if c.inspect != nil {
		return c.inspect(call)
	}
	if call >= len(c.inventories) {
		return status.Result{}, fmt.Errorf("unexpected inspect call %d", call+1)
	}
	return c.inventories[call], nil
}

func (c *fakeClient) UpdateNodeAvailability(_ context.Context, nodeID, availability string) error {
	c.updates = append(c.updates, availabilityUpdate{nodeID: nodeID, availability: availability})
	return nil
}

type fakeGuard struct {
	acquireCalls int
	ensureCalls  int
	released     bool
	ensureErr    error
}

func (g *fakeGuard) AcquireClusterLock(string) (func() error, error) {
	g.acquireCalls++
	return func() error {
		g.released = true
		return nil
	}, nil
}

func (g *fakeGuard) EnsureNoActiveOperation(string) error {
	g.ensureCalls++
	return g.ensureErr
}

func healthyDrainedInventory() status.Result {
	return status.Result{
		Endpoint: "unix:///var/run/docker.sock",
		Cluster:  status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "drain"},
		},
		Services: []status.Service{{ID: "s1", Name: "api", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Converged: true}},
	}
}

func findingMessages(findings []preflight.Finding, level preflight.Level) []string {
	messages := make([]string, 0, len(findings))
	for _, finding := range findings {
		if finding.Level == level {
			messages = append(messages, finding.Message)
		}
	}
	return messages
}
