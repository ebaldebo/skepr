package maintenance

import (
	"context"
	"fmt"
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

type endpointConnector struct {
	connections map[string]status.Connection
	attempts    []string
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

func (endpointConnection) Inspect(context.Context) (status.Result, error) {
	return status.Result{Cluster: status.Cluster{ID: "cluster-1"}}, nil
}

func (endpointConnection) Close() error { return nil }

func (failingEndpointConnection) Inspect(context.Context) (status.Result, error) {
	return status.Result{}, fmt.Errorf("connection reset")
}

func (failingEndpointConnection) Close() error { return nil }
