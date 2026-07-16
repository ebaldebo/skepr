package docker

import (
	"context"
	"fmt"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/client"
)

type engine interface {
	Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error)
	DaemonHost() string
}

type Inspector struct {
	engine engine
	close  func() error
}

func NewInspector() (*Inspector, error) {
	dockerClient, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("configure Docker client: %w", err)
	}
	inspector := newInspector(dockerClient)
	inspector.close = dockerClient.Close
	return inspector, nil
}

func newInspector(engine engine) *Inspector {
	return &Inspector{engine: engine}
}

func (i *Inspector) Inspect(ctx context.Context) (status.Result, error) {
	response, err := i.engine.Info(ctx, client.InfoOptions{})
	if err != nil {
		return status.Result{}, fmt.Errorf("query Docker Engine at %q: %w", i.engine.DaemonHost(), err)
	}

	clusterID := ""
	if response.Info.Swarm.Cluster != nil {
		clusterID = response.Info.Swarm.Cluster.ID
	}
	return status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      i.engine.DaemonHost(),
		Cluster: status.Cluster{
			ID:               clusterID,
			LocalState:       string(response.Info.Swarm.LocalNodeState),
			ControlAvailable: response.Info.Swarm.ControlAvailable,
		},
	}, nil
}

func (i *Inspector) Close() error {
	if i.close == nil {
		return nil
	}
	return i.close()
}
