package cli

import (
	"bytes"
	"context"
	"testing"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
)

func TestStatusPrintsClusterSummary(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"status"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, 0, exitCode)
	assert.Equal(t, `Cluster: cluster-1
Endpoint: unix:///var/run/docker.sock
Swarm: active
Control: available
Leader: manager-1

Nodes:
  manager-1  m1  manager  ready  active  leader
  manager-2  m2  manager  ready  active  reachable
  worker-1   w1  worker   ready  active
`, stdout.String())
}

func TestStatusJSONOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"status", "--json"}, &fakeConnector{}, &stdout, &bytes.Buffer{})

	assert.Equal(t, 0, exitCode)
	assert.JSONEq(t, `{
  "schema_version": 1,
  "endpoint": "unix:///var/run/docker.sock",
  "cluster": {
    "id": "cluster-1",
    "local_state": "active",
    "control_available": true
  },
  "leader": "manager-1",
  "nodes": [
    {
      "id": "m1",
      "hostname": "manager-1",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "leader"
    },
    {
      "id": "m2",
      "hostname": "manager-2",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "reachable"
    },
    {
      "id": "w1",
      "hostname": "worker-1",
      "role": "worker",
      "state": "ready",
      "availability": "active"
    }
  ]
}`, stdout.String())
	assert.Equal(t, `{
  "schema_version": 1,
  "endpoint": "unix:///var/run/docker.sock",
  "cluster": {
    "id": "cluster-1",
    "local_state": "active",
    "control_available": true
  },
  "leader": "manager-1",
  "nodes": [
    {
      "id": "m1",
      "hostname": "manager-1",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "leader"
    },
    {
      "id": "m2",
      "hostname": "manager-2",
      "role": "manager",
      "state": "ready",
      "availability": "active",
      "manager_status": "reachable"
    },
    {
      "id": "w1",
      "hostname": "worker-1",
      "role": "worker",
      "state": "ready",
      "availability": "active"
    }
  ]
}
`, stdout.String())
}

func TestStatusPutsUnavailableControlFirst(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	exitCode := Run(context.Background(), []string{"status"}, &fakeConnector{connection: unavailableInspector{}}, &stdout, &bytes.Buffer{})

	assert.Equal(t, 0, exitCode)
	assert.Equal(t, "UNSAFE: Swarm control is unavailable\nCluster: \nEndpoint: unix:///var/run/docker.sock\nSwarm: inactive\nControl: unavailable\n", stdout.String())
}

func TestStatusUsesExplicitDockerContext(t *testing.T) {
	t.Parallel()

	connector := &fakeConnector{}
	exitCode := Run(context.Background(), []string{"--context", "swarm", "status"}, connector, &bytes.Buffer{}, &bytes.Buffer{})

	assert.Equal(t, 0, exitCode)
	assert.Equal(t, "swarm", connector.contextName)
}

type fakeConnector struct {
	contextName string
	connection  status.Connection
}

func (f *fakeConnector) Connect(_ context.Context, contextName string) (status.Connection, error) {
	f.contextName = contextName
	if f.connection == nil {
		f.connection = fakeInspector{}
	}
	return f.connection, nil
}

type fakeInspector struct{}

func (fakeInspector) Inspect(context.Context) (status.Result, error) {
	return status.Result{
		SchemaVersion: 1,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster: status.Cluster{
			ID:               "cluster-1",
			LocalState:       "active",
			ControlAvailable: true,
		},
		Leader: "manager-1",
		Nodes: []status.Node{
			{ID: "m1", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
			{ID: "m2", Hostname: "manager-2", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "reachable"},
			{ID: "w1", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active"},
		},
	}, nil
}

func (fakeInspector) Close() error { return nil }

type unavailableInspector struct{}

func (unavailableInspector) Inspect(context.Context) (status.Result, error) {
	return status.Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      "unix:///var/run/docker.sock",
		Cluster:       status.Cluster{LocalState: "inactive"},
	}, nil
}

func (unavailableInspector) Close() error { return nil }
