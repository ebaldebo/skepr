package docker

import (
	"context"
	"fmt"
	"sort"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/client"
)

type engine interface {
	Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error)
	NodeList(context.Context, client.NodeListOptions) (client.NodeListResult, error)
	DaemonHost() string
}

type Inspector struct {
	engine   engine
	endpoint string
	close    func() error
}

func newInspector(engine engine) *Inspector {
	return newInspectorAt(engine, engine.DaemonHost())
}

func newInspectorAt(engine engine, endpoint string) *Inspector {
	return &Inspector{engine: engine, endpoint: endpoint}
}

func (i *Inspector) Inspect(ctx context.Context) (status.Result, error) {
	response, err := i.engine.Info(ctx, client.InfoOptions{})
	if err != nil {
		return status.Result{}, fmt.Errorf("query Docker Engine at %q: %w", i.endpoint, err)
	}

	clusterID := ""
	if response.Info.Swarm.Cluster != nil {
		clusterID = response.Info.Swarm.Cluster.ID
	}
	result := status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      i.endpoint,
		Cluster: status.Cluster{
			ID:               clusterID,
			LocalState:       string(response.Info.Swarm.LocalNodeState),
			ControlAvailable: response.Info.Swarm.ControlAvailable,
		},
	}
	if !result.Cluster.ControlAvailable {
		return result, nil
	}

	nodeResponse, err := i.engine.NodeList(ctx, client.NodeListOptions{})
	if err != nil {
		return status.Result{}, fmt.Errorf("query Swarm nodes at %q: %w", i.endpoint, err)
	}
	for _, node := range nodeResponse.Items {
		managerStatus := ""
		if node.ManagerStatus != nil {
			managerStatus = string(node.ManagerStatus.Reachability)
			if node.ManagerStatus.Leader {
				managerStatus = "leader"
				result.Leader = node.Description.Hostname
			}
		}
		result.Nodes = append(result.Nodes, status.Node{
			ID:            node.ID,
			Hostname:      node.Description.Hostname,
			Role:          string(node.Spec.Role),
			State:         string(node.Status.State),
			Availability:  string(node.Spec.Availability),
			ManagerStatus: managerStatus,
		})
	}
	sort.Slice(result.Nodes, func(a, b int) bool {
		return result.Nodes[a].Hostname < result.Nodes[b].Hostname
	})
	return result, nil
}

func (i *Inspector) Close() error {
	if i.close == nil {
		return nil
	}
	return i.close()
}
