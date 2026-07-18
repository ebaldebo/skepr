package maintenance

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointPoolFailsOverWhenPreferredContextCannotConnect(t *testing.T) {
	connector := &endpointConnector{connections: map[string]status.Connection{
		"manager-2": endpointConnection{},
	}}
	pool := NewEndpointPool(connector, []string{"manager-1", "manager-2"})

	result, err := pool.Inspect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "cluster-1", result.Cluster.ID)
	assert.Equal(t, []string{"manager-1", "manager-2"}, connector.attempts)
}

func TestEndpointPoolFailsOverWhenSelectedManagerStopsResponding(t *testing.T) {
	connector := &endpointConnector{connections: map[string]status.Connection{
		"manager-1": failingEndpointConnection{},
		"manager-2": endpointConnection{},
	}}
	pool := NewEndpointPool(connector, []string{"manager-1", "manager-2"})

	result, err := pool.Inspect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "cluster-1", result.Cluster.ID)
	assert.Equal(t, []string{"manager-1", "manager-2"}, connector.attempts)
}

func TestEndpointPoolConnectsRawSSHEndpointAfterContextFailure(t *testing.T) {
	connector := &endpointConnector{endpointConnections: map[string]status.Connection{
		"ssh://root@manager-2": endpointConnection{},
	}}
	pool := NewEndpointPoolWithEndpoints(connector, []string{"manager-1"}, []string{"ssh://root@manager-2"})

	result, err := pool.Inspect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "cluster-1", result.Cluster.ID)
	assert.Equal(t, []string{"manager-1"}, connector.attempts)
	assert.Equal(t, []string{"ssh://root@manager-2"}, connector.endpointAttempts)
}

func TestEndpointPoolReconcilesAmbiguousNodeMutationBeforeRetry(t *testing.T) {
	state := &mutationState{availability: "active"}
	first := &mutationEndpointConnection{state: state, failNodeAfterApply: true}
	second := &mutationEndpointConnection{state: state}
	connector := &endpointConnector{connections: map[string]status.Connection{"manager-1": first, "manager-2": second}}
	pool := NewEndpointPool(connector, []string{"manager-1", "manager-2"})
	_, err := pool.Inspect(context.Background())
	require.NoError(t, err)

	err = pool.UpdateNodeAvailability(context.Background(), "worker-id", "drain")

	require.NoError(t, err)
	assert.Equal(t, 1, first.nodeUpdates)
	assert.Equal(t, 0, second.nodeUpdates)
}

func TestEndpointPoolRetriesUnappliedServiceMutationOnFailover(t *testing.T) {
	state := &mutationState{availability: "drain"}
	first := &mutationEndpointConnection{state: state, failForceBeforeApply: true}
	second := &mutationEndpointConnection{state: state}
	connector := &endpointConnector{connections: map[string]status.Connection{"manager-1": first, "manager-2": second}}
	pool := NewEndpointPool(connector, []string{"manager-1", "manager-2"})
	_, err := pool.Inspect(context.Background())
	require.NoError(t, err)

	err = pool.ForceUpdateService(context.Background(), "service-1")

	require.NoError(t, err)
	assert.Equal(t, 1, first.forceUpdates)
	assert.Equal(t, 1, second.forceUpdates)
	state.mu.Lock()
	assert.Equal(t, uint64(1), state.forceUpdate)
	state.mu.Unlock()
}

func TestEndpointPoolDoesNotRepeatAppliedServiceMutationOnFailover(t *testing.T) {
	state := &mutationState{availability: "drain"}
	first := &mutationEndpointConnection{state: state, failForceAfterApply: true}
	second := &mutationEndpointConnection{state: state}
	connector := &endpointConnector{connections: map[string]status.Connection{"manager-1": first, "manager-2": second}}
	pool := NewEndpointPool(connector, []string{"manager-1", "manager-2"})
	_, err := pool.Inspect(context.Background())
	require.NoError(t, err)

	err = pool.ForceUpdateService(context.Background(), "service-1")

	require.NoError(t, err)
	assert.Equal(t, 1, first.forceUpdates)
	assert.Equal(t, 0, second.forceUpdates)
}

func TestEndpointPoolRejectsFailoverCandidateFromAnotherSwarm(t *testing.T) {
	connector := &endpointConnector{connections: map[string]status.Connection{
		"wrong": statusEndpointConnection{clusterID: "cluster-2"},
		"right": statusEndpointConnection{clusterID: "cluster-1"},
	}}
	pool := NewEndpointPool(connector, []string{"wrong", "right"})
	pool.PinCluster("cluster-1")

	result, err := pool.Inspect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "cluster-1", result.Cluster.ID)
	assert.Equal(t, []string{"wrong", "right"}, connector.attempts)
}

type endpointConnector struct {
	connections         map[string]status.Connection
	endpointConnections map[string]status.Connection
	attempts            []string
	endpointAttempts    []string
}

func (c *endpointConnector) ConnectEndpoint(_ context.Context, endpoint string) (status.Connection, error) {
	c.endpointAttempts = append(c.endpointAttempts, endpoint)
	connection, exists := c.endpointConnections[endpoint]
	if !exists {
		return nil, fmt.Errorf("unreachable")
	}
	return connection, nil
}

func (c *endpointConnector) Connect(_ context.Context, contextName string) (status.Connection, error) {
	c.attempts = append(c.attempts, contextName)
	connection, exists := c.connections[contextName]
	if !exists {
		return nil, fmt.Errorf("unreachable")
	}
	return connection, nil
}

type endpointConnection struct{}

type failingEndpointConnection struct{}

type statusEndpointConnection struct {
	clusterID string
}

func (endpointConnection) Inspect(context.Context) (status.Result, error) {
	return status.Result{Cluster: status.Cluster{ID: "cluster-1"}}, nil
}

func (endpointConnection) Close() error { return nil }

func (failingEndpointConnection) Inspect(context.Context) (status.Result, error) {
	return status.Result{}, fmt.Errorf("connection reset")
}

func (failingEndpointConnection) Close() error { return nil }

func (c statusEndpointConnection) Inspect(context.Context) (status.Result, error) {
	return status.Result{Cluster: status.Cluster{ID: c.clusterID}}, nil
}

func (statusEndpointConnection) Close() error { return nil }

type mutationState struct {
	mu           sync.Mutex
	availability string
	forceUpdate  uint64
}

type mutationEndpointConnection struct {
	state                *mutationState
	failNodeAfterApply   bool
	failForceBeforeApply bool
	failForceAfterApply  bool
	nodeUpdates          int
	forceUpdates         int
}

func (c *mutationEndpointConnection) Inspect(context.Context) (status.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	return status.Result{
		Cluster:  status.Cluster{ID: "cluster-1", LocalState: "active", ControlAvailable: true},
		Nodes:    []status.Node{{ID: "worker-id", Hostname: "worker-1", State: "ready", Availability: c.state.availability}},
		Services: []status.Service{{ID: "service-1", Name: "service-1", ForceUpdate: c.state.forceUpdate}},
	}, nil
}

func (c *mutationEndpointConnection) UpdateNodeAvailability(_ context.Context, _ string, availability string) error {
	c.nodeUpdates++
	c.state.mu.Lock()
	c.state.availability = availability
	c.state.mu.Unlock()
	if c.failNodeAfterApply {
		return fmt.Errorf("connection reset after request")
	}
	return nil
}

func (c *mutationEndpointConnection) ForceUpdateService(context.Context, string) error {
	c.forceUpdates++
	if c.failForceBeforeApply {
		return fmt.Errorf("connection reset before request")
	}
	c.state.mu.Lock()
	c.state.forceUpdate++
	c.state.mu.Unlock()
	if c.failForceAfterApply {
		return fmt.Errorf("connection reset after request")
	}
	return nil
}

func (c *mutationEndpointConnection) Close() error { return nil }
