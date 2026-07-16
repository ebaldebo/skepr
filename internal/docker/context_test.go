package docker

import (
	"context"
	"errors"
	"testing"

	contextdocker "github.com/docker/cli/cli/context/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextResolverPrecedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		explicit    string
		environment map[string]string
		active      string
		want        string
	}{
		{
			name:     "explicit context",
			explicit: "explicit",
			environment: map[string]string{
				"DOCKER_HOST":    "tcp://manager:2376",
				"DOCKER_CONTEXT": "environment",
			},
			active: "active",
			want:   "explicit",
		},
		{
			name: "docker host uses default context",
			environment: map[string]string{
				"DOCKER_HOST":    "tcp://manager:2376",
				"DOCKER_CONTEXT": "environment",
			},
			active: "active",
			want:   defaultContextName,
		},
		{
			name:        "environment context",
			environment: map[string]string{"DOCKER_CONTEXT": "environment"},
			active:      "active",
			want:        "environment",
		},
		{
			name:   "active context",
			active: "active",
			want:   "active",
		},
		{
			name: "default context",
			want: defaultContextName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resolver := contextResolver{
				getenv: func(name string) string { return tt.environment[name] },
				currentContext: func() (string, error) {
					return tt.active, nil
				},
			}

			name, err := resolver.resolveName(tt.explicit)

			require.NoError(t, err)
			assert.Equal(t, tt.want, name)
		})
	}
}

func TestConnectorConfiguresSSHContext(t *testing.T) {
	t.Parallel()

	connector := &Connector{resolver: contextResolver{
		getenv: func(string) string { return "" },
		currentContext: func() (string, error) {
			return "swarm", nil
		},
		loadEndpoint: func(string) (contextdocker.Endpoint, error) {
			return contextdocker.Endpoint{
				EndpointMeta: contextdocker.EndpointMeta{Host: "ssh://user@manager"},
			}, nil
		},
	}}

	statusInspector, err := connector.Connect(context.Background(), "")

	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, statusInspector.Close()) })
	inspector := statusInspector.(*Inspector)
	assert.Equal(t, "ssh://user@manager", inspector.endpoint)
	assert.Equal(t, "http://docker.example.com", inspector.engine.DaemonHost())
}

func TestContextResolverReturnsConfigError(t *testing.T) {
	t.Parallel()

	want := errors.New("broken Docker config")
	resolver := contextResolver{
		getenv: func(string) string { return "" },
		currentContext: func() (string, error) {
			return "", want
		},
	}

	_, err := resolver.resolveName("")

	require.ErrorIs(t, err, want)
}
