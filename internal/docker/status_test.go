package docker

import (
	"context"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/api/types/swarm"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInspectorNormalizesSwarmStatus(t *testing.T) {
	t.Parallel()

	inspector := newInspector(fakeEngine{
		host: "unix:///var/run/docker.sock",
		info: system.Info{Swarm: swarm.Info{
			LocalNodeState:   swarm.LocalNodeStateActive,
			ControlAvailable: true,
			Cluster:          &swarm.ClusterInfo{ID: "cluster-1"},
		}},
	})

	result, err := inspector.Inspect(context.Background())

	require.NoError(t, err)
	assert.Equal(t, status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster: status.Cluster{
			ID:               "cluster-1",
			LocalState:       "active",
			ControlAvailable: true,
		},
	}, result)
}

type fakeEngine struct {
	host string
	info system.Info
}

func (f fakeEngine) Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error) {
	return client.SystemInfoResult{Info: f.info}, nil
}

func (f fakeEngine) DaemonHost() string {
	return f.host
}
