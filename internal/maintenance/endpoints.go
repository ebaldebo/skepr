package maintenance

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/ebaldebo/skepr/internal/status"
)

type EndpointPool struct {
	connector  status.Connector
	candidates []endpointCandidate
	connection status.Connection
	current    int
	lastResult status.Result
	hasResult  bool
	clusterID  string
}

type endpointCandidate struct {
	value string
	raw   bool
}

func (c endpointCandidate) label() string {
	if !c.raw {
		return c.value
	}
	parsed, err := url.Parse(c.value)
	if err != nil {
		return "ssh://invalid-endpoint"
	}
	if parsed.User != nil {
		parsed.User = url.User(parsed.User.Username())
	}
	return parsed.String()
}

func NewEndpointPool(connector status.Connector, contexts []string) *EndpointPool {
	return NewEndpointPoolWithEndpoints(connector, contexts, nil)
}

func NewEndpointPoolWithEndpoints(connector status.Connector, contexts, endpoints []string) *EndpointPool {
	if len(contexts) == 0 && len(endpoints) == 0 {
		contexts = []string{""}
	}
	candidates := make([]endpointCandidate, 0, len(contexts)+len(endpoints))
	for _, contextName := range contexts {
		candidates = append(candidates, endpointCandidate{value: contextName})
	}
	for _, endpoint := range endpoints {
		candidates = append(candidates, endpointCandidate{value: endpoint, raw: true})
	}
	return &EndpointPool{connector: connector, candidates: candidates, current: -1}
}

func (p *EndpointPool) Inspect(ctx context.Context) (status.Result, error) {
	var failures []error
	for attempts := 0; attempts < len(p.candidates); attempts++ {
		if p.connection == nil {
			if err := p.connect(ctx, p.nextIndex()); err != nil {
				failures = append(failures, err)
				continue
			}
		}
		result, err := p.connection.Inspect(ctx)
		if err == nil {
			if p.clusterID != "" && result.Cluster.ID != p.clusterID {
				failures = append(failures, fmt.Errorf("docker endpoint %q belongs to Swarm %s, expected %s", p.candidates[p.current].label(), result.Cluster.ID, p.clusterID))
				p.invalidateCurrent()
				continue
			}
			if p.clusterID == "" {
				p.clusterID = result.Cluster.ID
			}
			p.lastResult = result
			p.hasResult = true
			return result, nil
		}
		failures = append(failures, fmt.Errorf("inspect Docker endpoint %q: %w", p.candidates[p.current].label(), err))
		_ = p.connection.Close()
		p.connection = nil
	}
	return status.Result{}, fmt.Errorf("all Docker manager endpoints failed: %w", errors.Join(failures...))
}

func (p *EndpointPool) PinCluster(clusterID string) {
	p.clusterID = clusterID
}

func (p *EndpointPool) UpdateNodeAvailability(ctx context.Context, nodeID, availability string) error {
	connection, ok := p.connection.(status.MaintenanceConnection)
	if !ok {
		return fmt.Errorf("current Docker endpoint does not support Swarm node maintenance")
	}
	mutationErr := connection.UpdateNodeAvailability(ctx, nodeID, availability)
	if mutationErr == nil {
		return nil
	}
	p.invalidateCurrent()
	inventory, inspectErr := p.Inspect(ctx)
	if inspectErr != nil {
		return fmt.Errorf("reconcile failed node availability update: %w", errors.Join(mutationErr, inspectErr))
	}
	if nodeAvailability(inventory, nodeID) == availability {
		return nil
	}
	connection, ok = p.connection.(status.MaintenanceConnection)
	if !ok {
		return fmt.Errorf("failover Docker endpoint does not support Swarm node maintenance")
	}
	retryErr := connection.UpdateNodeAvailability(ctx, nodeID, availability)
	if retryErr == nil {
		return nil
	}
	p.invalidateCurrent()
	inventory, inspectErr = p.Inspect(ctx)
	if inspectErr == nil && nodeAvailability(inventory, nodeID) == availability {
		return nil
	}
	return fmt.Errorf("node availability update failed after reconciled failover: %w", errors.Join(mutationErr, retryErr, inspectErr))
}

func (p *EndpointPool) ForceUpdateService(ctx context.Context, serviceID string) error {
	if !p.hasResult {
		if _, err := p.Inspect(ctx); err != nil {
			return fmt.Errorf("inspect service before force update: %w", err)
		}
	}
	previous, exists := serviceForceUpdate(p.lastResult, serviceID)
	if !exists {
		return fmt.Errorf("service %s is missing before force update", serviceID)
	}
	connection, ok := p.connection.(status.ReconciliationConnection)
	if !ok {
		return fmt.Errorf("current Docker endpoint does not support Swarm service reconciliation")
	}
	mutationErr := connection.ForceUpdateService(ctx, serviceID)
	if mutationErr == nil {
		return nil
	}
	p.invalidateCurrent()
	inventory, inspectErr := p.Inspect(ctx)
	if inspectErr != nil {
		return fmt.Errorf("reconcile failed service force update: %w", errors.Join(mutationErr, inspectErr))
	}
	current, exists := serviceForceUpdate(inventory, serviceID)
	if !exists {
		return fmt.Errorf("service %s is missing after failed force update", serviceID)
	}
	if current > previous {
		return nil
	}
	if current < previous {
		return fmt.Errorf("service %s force-update counter moved backwards from %d to %d", serviceID, previous, current)
	}
	connection, ok = p.connection.(status.ReconciliationConnection)
	if !ok {
		return fmt.Errorf("failover Docker endpoint does not support Swarm service reconciliation")
	}
	retryErr := connection.ForceUpdateService(ctx, serviceID)
	if retryErr == nil {
		return nil
	}
	p.invalidateCurrent()
	inventory, inspectErr = p.Inspect(ctx)
	if inspectErr == nil {
		current, exists = serviceForceUpdate(inventory, serviceID)
		if exists && current > previous {
			return nil
		}
	}
	return fmt.Errorf("service force update failed after reconciled failover: %w", errors.Join(mutationErr, retryErr, inspectErr))
}

func (p *EndpointPool) Close() error {
	if p.connection == nil {
		return nil
	}
	return p.connection.Close()
}

func (p *EndpointPool) connect(ctx context.Context, index int) error {
	p.current = index
	candidate := p.candidates[index]
	var connection status.Connection
	var err error
	if candidate.raw {
		endpointConnector, ok := p.connector.(status.EndpointConnector)
		if !ok {
			return fmt.Errorf("docker connector does not support raw endpoint %q", candidate.label())
		}
		connection, err = endpointConnector.ConnectEndpoint(ctx, candidate.value)
	} else {
		connection, err = p.connector.Connect(ctx, candidate.value)
	}
	if err != nil {
		return fmt.Errorf("connect Docker endpoint %q: %w", candidate.label(), err)
	}
	p.connection = connection
	return nil
}

func (p *EndpointPool) nextIndex() int {
	return (p.current + 1) % len(p.candidates)
}

func (p *EndpointPool) invalidateCurrent() {
	if p.connection != nil {
		_ = p.connection.Close()
	}
	p.connection = nil
	p.hasResult = false
}

func nodeAvailability(inventory status.Result, nodeID string) string {
	for _, node := range inventory.Nodes {
		if node.ID == nodeID {
			return node.Availability
		}
	}
	return ""
}

func serviceForceUpdate(inventory status.Result, serviceID string) (uint64, bool) {
	for _, service := range inventory.Services {
		if service.ID == serviceID {
			return service.ForceUpdate, true
		}
	}
	return 0, false
}
