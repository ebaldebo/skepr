package maintenance

import (
	"context"
	"errors"
	"fmt"

	"github.com/ebaldebo/skepr/internal/status"
)

type EndpointPool struct {
	connector  status.Connector
	contexts   []string
	connection status.Connection
	current    int
}

func NewEndpointPool(connector status.Connector, contexts []string) *EndpointPool {
	if len(contexts) == 0 {
		contexts = []string{""}
	}
	return &EndpointPool{connector: connector, contexts: append([]string(nil), contexts...), current: -1}
}

func (p *EndpointPool) Inspect(ctx context.Context) (status.Result, error) {
	var failures []error
	for attempts := 0; attempts < len(p.contexts); attempts++ {
		if p.connection == nil {
			if err := p.connect(ctx, p.nextIndex()); err != nil {
				failures = append(failures, err)
				continue
			}
		}
		result, err := p.connection.Inspect(ctx)
		if err == nil {
			return result, nil
		}
		failures = append(failures, fmt.Errorf("inspect Docker context %q: %w", p.contexts[p.current], err))
		_ = p.connection.Close()
		p.connection = nil
	}
	return status.Result{}, fmt.Errorf("all Docker manager endpoints failed: %w", errors.Join(failures...))
}

func (p *EndpointPool) UpdateNodeAvailability(ctx context.Context, nodeID, availability string) error {
	connection, ok := p.connection.(status.MaintenanceConnection)
	if !ok {
		return fmt.Errorf("current Docker endpoint does not support Swarm node maintenance")
	}
	return connection.UpdateNodeAvailability(ctx, nodeID, availability)
}

func (p *EndpointPool) ForceUpdateService(ctx context.Context, serviceID string) error {
	connection, ok := p.connection.(status.ReconciliationConnection)
	if !ok {
		return fmt.Errorf("current Docker endpoint does not support Swarm service reconciliation")
	}
	return connection.ForceUpdateService(ctx, serviceID)
}

func (p *EndpointPool) Close() error {
	if p.connection == nil {
		return nil
	}
	return p.connection.Close()
}

func (p *EndpointPool) connect(ctx context.Context, index int) error {
	p.current = index
	connection, err := p.connector.Connect(ctx, p.contexts[index])
	if err != nil {
		return fmt.Errorf("connect Docker context %q: %w", p.contexts[index], err)
	}
	p.connection = connection
	return nil
}

func (p *EndpointPool) nextIndex() int {
	return (p.current + 1) % len(p.contexts)
}
