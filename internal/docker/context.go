package docker

import (
	"context"
	"fmt"
	"os"
	"strings"

	dockerconfig "github.com/docker/cli/cli/config"
	contextdocker "github.com/docker/cli/cli/context/docker"
	"github.com/docker/cli/cli/context/store"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/client"
)

const defaultContextName = "default"

type Connector struct {
	resolver contextResolver
}

func NewConnector() *Connector {
	return &Connector{resolver: newContextResolver()}
}

func (c *Connector) Connect(_ context.Context, explicitContext string) (status.Connection, error) {
	resolved, err := c.resolver.resolve(explicitContext)
	if err != nil {
		return nil, err
	}

	var dockerClient *client.Client
	if resolved.endpoint == nil {
		var options []client.Opt
		if resolved.useEnvironment {
			options = append(options, client.FromEnv)
		}
		dockerClient, err = client.New(options...)
	} else {
		options, optionsErr := resolved.endpoint.ClientOpts()
		if optionsErr != nil {
			return nil, fmt.Errorf("configure Docker context %q: %w", resolved.name, optionsErr)
		}
		dockerClient, err = client.New(options...)
	}
	if err != nil {
		return nil, fmt.Errorf("create Docker client for context %q: %w", resolved.name, err)
	}

	endpoint := dockerClient.DaemonHost()
	if resolved.endpoint != nil {
		endpoint = resolved.endpoint.Host
	}
	inspector := newInspectorAt(dockerClient, endpoint)
	inspector.close = dockerClient.Close
	return inspector, nil
}

type resolvedContext struct {
	name           string
	endpoint       *contextdocker.Endpoint
	useEnvironment bool
}

type contextResolver struct {
	getenv         func(string) string
	currentContext func() (string, error)
	loadEndpoint   func(string) (contextdocker.Endpoint, error)
}

func newContextResolver() contextResolver {
	return contextResolver{
		getenv: os.Getenv,
		currentContext: func() (string, error) {
			configuration, err := dockerconfig.Load(dockerconfig.Dir())
			if err != nil {
				return "", fmt.Errorf("load Docker configuration: %w", err)
			}
			return configuration.CurrentContext, nil
		},
		loadEndpoint: loadContextEndpoint,
	}
}

func (r contextResolver) resolve(explicit string) (resolvedContext, error) {
	name, err := r.resolveName(explicit)
	if err != nil {
		return resolvedContext{}, err
	}
	if name == defaultContextName {
		if explicit != "" {
			return resolvedContext{name: name}, nil
		}
		host := r.getenv(client.EnvOverrideHost)
		if strings.HasPrefix(strings.ToLower(host), "ssh://") {
			endpoint := contextdocker.Endpoint{EndpointMeta: contextdocker.EndpointMeta{Host: host}}
			return resolvedContext{name: name, endpoint: &endpoint}, nil
		}
		return resolvedContext{name: name, useEnvironment: true}, nil
	}
	endpoint, err := r.loadEndpoint(name)
	if err != nil {
		return resolvedContext{}, fmt.Errorf("resolve Docker context %q: %w", name, err)
	}
	return resolvedContext{name: name, endpoint: &endpoint}, nil
}

func (r contextResolver) resolveName(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if r.getenv(client.EnvOverrideHost) != "" {
		return defaultContextName, nil
	}
	if name := r.getenv("DOCKER_CONTEXT"); name != "" {
		return name, nil
	}
	name, err := r.currentContext()
	if err != nil {
		return "", err
	}
	if name == "" {
		return defaultContextName, nil
	}
	return name, nil
}

func loadContextEndpoint(name string) (contextdocker.Endpoint, error) {
	storeConfig := store.NewConfig(nil, store.EndpointTypeGetter(
		contextdocker.DockerEndpoint,
		func() any { return &contextdocker.EndpointMeta{} },
	))
	contextStore := store.New(dockerconfig.ContextStoreDir(), storeConfig)
	metadata, err := contextStore.GetMetadata(name)
	if err != nil {
		return contextdocker.Endpoint{}, err
	}
	endpointMetadata, err := contextdocker.EndpointFromContext(metadata)
	if err != nil {
		return contextdocker.Endpoint{}, err
	}
	return contextdocker.WithTLSData(contextStore, name, endpointMetadata)
}
