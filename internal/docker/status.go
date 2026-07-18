package docker

import (
	"context"
	"fmt"
	"sort"

	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/api/types/swarm"
	"github.com/moby/moby/client"
)

type engine interface {
	Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error)
	NodeList(context.Context, client.NodeListOptions) (client.NodeListResult, error)
	ServiceList(context.Context, client.ServiceListOptions) (client.ServiceListResult, error)
	TaskList(context.Context, client.TaskListOptions) (client.TaskListResult, error)
	DaemonHost() string
}

type nodeMutator interface {
	NodeInspect(context.Context, string, client.NodeInspectOptions) (client.NodeInspectResult, error)
	NodeUpdate(context.Context, string, client.NodeUpdateOptions) (client.NodeUpdateResult, error)
}

type serviceMutator interface {
	ServiceInspect(context.Context, string, client.ServiceInspectOptions) (client.ServiceInspectResult, error)
	ServiceUpdate(context.Context, string, client.ServiceUpdateOptions) (client.ServiceUpdateResult, error)
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
	nodeNames := make(map[string]string, len(nodeResponse.Items))
	for _, node := range nodeResponse.Items {
		nodeNames[node.ID] = node.Description.Hostname
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

	serviceResponse, err := i.engine.ServiceList(ctx, client.ServiceListOptions{Status: true})
	if err != nil {
		return status.Result{}, fmt.Errorf("query Swarm services at %q: %w", i.endpoint, err)
	}
	serviceNames := make(map[string]string, len(serviceResponse.Items))
	serviceModes := make(map[string]string, len(serviceResponse.Items))
	for _, service := range serviceResponse.Items {
		if service.ServiceStatus == nil {
			return status.Result{}, fmt.Errorf("query Swarm services at %q: service %q has no task counts", i.endpoint, service.Spec.Name)
		}
		mode := "unknown"
		switch {
		case service.Spec.Mode.Replicated != nil:
			mode = "replicated"
		case service.Spec.Mode.Global != nil:
			mode = "global"
		case service.Spec.Mode.ReplicatedJob != nil:
			mode = "replicated-job"
		case service.Spec.Mode.GlobalJob != nil:
			mode = "global-job"
		}
		serviceNames[service.ID] = service.Spec.Name
		serviceModes[service.ID] = mode
		result.Services = append(result.Services, status.Service{
			ID:           service.ID,
			Name:         service.Spec.Name,
			Mode:         mode,
			RunningTasks: service.ServiceStatus.RunningTasks,
			DesiredTasks: service.ServiceStatus.DesiredTasks,
			Converged:    service.ServiceStatus.RunningTasks == service.ServiceStatus.DesiredTasks,
		})
	}
	sort.Slice(result.Services, func(a, b int) bool {
		if result.Services[a].Converged != result.Services[b].Converged {
			return !result.Services[a].Converged
		}
		return result.Services[a].Name < result.Services[b].Name
	})

	taskFilters := make(client.Filters).Add("desired-state", string(swarm.TaskStateRunning))
	taskResponse, err := i.engine.TaskList(ctx, client.TaskListOptions{Filters: taskFilters})
	if err != nil {
		return status.Result{}, fmt.Errorf("query Swarm tasks at %q: %w", i.endpoint, err)
	}
	for _, task := range taskResponse.Items {
		if task.DesiredState != swarm.TaskStateRunning {
			continue
		}
		serviceName := serviceNames[task.ServiceID]
		if serviceName == "" {
			serviceName = task.ServiceID
		}
		nodeName := nodeNames[task.NodeID]
		if nodeName == "" {
			nodeName = task.NodeID
		}
		if nodeName == "" {
			nodeName = "unassigned"
		}
		taskName := serviceName + "." + nodeName
		if serviceModes[task.ServiceID] == "replicated" || serviceModes[task.ServiceID] == "replicated-job" {
			taskName = fmt.Sprintf("%s.%d", serviceName, task.Slot)
		}
		normalizedTask := status.Task{
			ID:           task.ID,
			Name:         taskName,
			ServiceID:    task.ServiceID,
			Service:      serviceName,
			NodeID:       task.NodeID,
			Node:         nodeName,
			DesiredState: string(task.DesiredState),
			State:        string(task.Status.State),
			Error:        task.Status.Err,
		}
		result.DesiredTasks = append(result.DesiredTasks, normalizedTask)
		if unhealthyTaskState(task.Status.State) {
			result.UnhealthyTasks = append(result.UnhealthyTasks, normalizedTask)
		}
	}
	sort.Slice(result.DesiredTasks, func(a, b int) bool {
		if result.DesiredTasks[a].Name != result.DesiredTasks[b].Name {
			return result.DesiredTasks[a].Name < result.DesiredTasks[b].Name
		}
		return result.DesiredTasks[a].ID < result.DesiredTasks[b].ID
	})
	sort.Slice(result.UnhealthyTasks, func(a, b int) bool {
		if result.UnhealthyTasks[a].Name != result.UnhealthyTasks[b].Name {
			return result.UnhealthyTasks[a].Name < result.UnhealthyTasks[b].Name
		}
		return result.UnhealthyTasks[a].ID < result.UnhealthyTasks[b].ID
	})
	return result, nil
}

func unhealthyTaskState(state swarm.TaskState) bool {
	switch state {
	case swarm.TaskStateComplete,
		swarm.TaskStateShutdown,
		swarm.TaskStateFailed,
		swarm.TaskStateRejected,
		swarm.TaskStateRemove,
		swarm.TaskStateOrphaned:
		return true
	default:
		return false
	}
}

func (i *Inspector) UpdateNodeAvailability(ctx context.Context, nodeID, availability string) error {
	mutator, ok := i.engine.(nodeMutator)
	if !ok {
		return fmt.Errorf("docker connection at %q does not support node updates", i.endpoint)
	}
	var desired swarm.NodeAvailability
	switch availability {
	case "active":
		desired = swarm.NodeAvailabilityActive
	case "pause":
		desired = swarm.NodeAvailabilityPause
	case "drain":
		desired = swarm.NodeAvailabilityDrain
	default:
		return fmt.Errorf("unsupported node availability %q", availability)
	}
	inspected, err := mutator.NodeInspect(ctx, nodeID, client.NodeInspectOptions{})
	if err != nil {
		return fmt.Errorf("inspect Swarm node %s at %q: %w", nodeID, i.endpoint, err)
	}
	spec := inspected.Node.Spec
	spec.Availability = desired
	if _, err := mutator.NodeUpdate(ctx, nodeID, client.NodeUpdateOptions{
		Version: inspected.Node.Version,
		Spec:    spec,
	}); err != nil {
		return fmt.Errorf("update Swarm node %s availability at %q: %w", nodeID, i.endpoint, err)
	}
	return nil
}

func (i *Inspector) ForceUpdateService(ctx context.Context, serviceID string) error {
	mutator, ok := i.engine.(serviceMutator)
	if !ok {
		return fmt.Errorf("docker connection at %q does not support service updates", i.endpoint)
	}
	inspected, err := mutator.ServiceInspect(ctx, serviceID, client.ServiceInspectOptions{})
	if err != nil {
		return fmt.Errorf("inspect Swarm service %s at %q: %w", serviceID, i.endpoint, err)
	}
	spec := inspected.Service.Spec
	spec.TaskTemplate.ForceUpdate++
	if _, err := mutator.ServiceUpdate(ctx, serviceID, client.ServiceUpdateOptions{
		Version:          inspected.Service.Version,
		Spec:             spec,
		RegistryAuthFrom: swarm.RegistryAuthFromSpec,
	}); err != nil {
		return fmt.Errorf("force update Swarm service %s at %q: %w", serviceID, i.endpoint, err)
	}
	return nil
}

func (i *Inspector) Close() error {
	if i.close == nil {
		return nil
	}
	return i.close()
}
