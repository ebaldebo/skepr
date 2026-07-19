package drain

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ebaldebo/skepr/internal/status"
)

const ResultSchemaVersion = 1

const (
	PhaseDraining        = "draining"
	PhaseEvacuating      = "evacuating"
	PhaseWaitingServices = "waiting-services"
	PhaseDrained         = "drained"
)

const (
	defaultDrainTimeout = 5 * time.Minute
	defaultPollInterval = time.Second
)

type Client interface {
	Inspect(context.Context) (status.Result, error)
	UpdateNodeAvailability(context.Context, string, string) error
}

type Guard interface {
	AcquireClusterLock(string) (func() error, error)
	EnsureNoActiveOperation(string) error
}

type AffectedService struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Mode string `json:"mode"`
}

type Result struct {
	SchemaVersion        int               `json:"schema_version"`
	Endpoint             string            `json:"endpoint"`
	ClusterID            string            `json:"cluster_id"`
	Target               status.Node       `json:"target"`
	Phase                string            `json:"phase"`
	MutationOccurred     bool              `json:"mutation_occurred"`
	Availability         string            `json:"availability"`
	ReplicatedTasksMoved int               `json:"replicated_tasks_moved"`
	GlobalTasksStopped   int               `json:"global_tasks_stopped"`
	AffectedServices     []AffectedService `json:"affected_services"`
	Evacuated            bool              `json:"evacuated"`
	ServicesConverged    bool              `json:"services_converged"`
}

type Drainer struct {
	Client       Client
	Guard        Guard
	Timeout      time.Duration
	PollInterval time.Duration
	Progress     func(Result)
}

type SafetyError struct {
	Preview Preview
}

func (e *SafetyError) Error() string {
	return fmt.Sprintf("node drain safety checks blocked target %s", e.Preview.RequestedNode)
}

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

type MutationError struct {
	Result Result
	Err    error
}

func (e *MutationError) Error() string {
	return fmt.Sprintf("node drain failed in phase %s: %v", e.Result.Phase, e.Err)
}

func (e *MutationError) Unwrap() error {
	return e.Err
}

func (d Drainer) Drain(ctx context.Context, requestedNode string) (Result, error) {
	initialInventory, err := d.Client.Inspect(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("inspect Swarm before node drain: %w", err)
	}
	initialPreview := BuildPreview(initialInventory, requestedNode)
	if !initialPreview.SafeToDrain {
		return Result{}, &SafetyError{Preview: initialPreview}
	}
	if d.Guard == nil {
		return Result{}, fmt.Errorf("node drain operation guard is required")
	}
	release, err := d.Guard.AcquireClusterLock(initialInventory.Cluster.ID)
	if err != nil {
		return Result{}, &ValidationError{Message: fmt.Sprintf("acquire node drain lock: %v", err)}
	}
	defer func() { _ = release() }()
	if err := d.Guard.EnsureNoActiveOperation(initialInventory.Cluster.ID); err != nil {
		return Result{}, &ValidationError{Message: err.Error()}
	}

	inventory, err := d.Client.Inspect(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("repeat Swarm inspection before node drain: %w", err)
	}
	preview := BuildPreview(inventory, requestedNode)
	if !preview.SafeToDrain {
		return Result{}, &SafetyError{Preview: preview}
	}
	if inventory.Cluster.ID != initialInventory.Cluster.ID {
		return Result{}, &ValidationError{Message: fmt.Sprintf("connected Swarm changed from %s to %s before node drain", initialInventory.Cluster.ID, inventory.Cluster.ID)}
	}
	if preview.Target == nil || initialPreview.Target == nil || preview.Target.ID != initialPreview.Target.ID {
		return Result{}, &ValidationError{Message: fmt.Sprintf("target node %s changed before node drain", requestedNode)}
	}

	result := Result{
		SchemaVersion:        ResultSchemaVersion,
		Endpoint:             inventory.Endpoint,
		ClusterID:            inventory.Cluster.ID,
		Target:               *preview.Target,
		Phase:                PhaseDraining,
		MutationOccurred:     true,
		Availability:         preview.Target.Availability,
		ReplicatedTasksMoved: len(preview.ReplicatedTasks),
		GlobalTasksStopped:   len(preview.GlobalTasks),
		AffectedServices:     affectedServices(inventory.Services, preview),
	}
	d.reportProgress(result)
	mutationCtx, cancel := context.WithTimeout(ctx, d.timeout())
	defer cancel()
	if err := d.Client.UpdateNodeAvailability(mutationCtx, result.Target.ID, "drain"); err != nil {
		return result, &MutationError{Result: result, Err: fmt.Errorf("request drain for target node %s: %w", result.Target.Hostname, err)}
	}

	result.Phase = PhaseEvacuating
	d.reportProgress(result)
	evacuatedInventory, err := d.waitForEvacuation(mutationCtx, result)
	if err != nil {
		return result, &MutationError{Result: result, Err: fmt.Errorf("wait for target evacuation: %w", err)}
	}
	result.Evacuated = true
	result.Availability = "drain"
	result.Phase = PhaseWaitingServices
	d.reportProgress(result)
	finalInventory, err := d.waitForAffectedServices(mutationCtx, result, evacuatedInventory)
	if err != nil {
		return result, &MutationError{Result: result, Err: fmt.Errorf("wait for affected services: %w", err)}
	}
	result.ServicesConverged = true
	if target, found := resolveTarget(finalInventory.Nodes, result.Target.ID); found {
		result.Target = target
		result.Availability = target.Availability
	}
	result.Phase = PhaseDrained
	d.reportProgress(result)
	return result, nil
}

func (d Drainer) waitForEvacuation(ctx context.Context, result Result) (status.Result, error) {
	for {
		inventory, err := d.Client.Inspect(ctx)
		if err != nil {
			return status.Result{}, err
		}
		if err := validateLiveTarget(inventory, result); err != nil {
			return status.Result{}, err
		}
		target, _ := resolveTarget(inventory.Nodes, result.Target.ID)
		evacuated := target.Availability == "drain"
		for _, task := range inventory.DesiredTasks {
			if task.NodeID == result.Target.ID && task.DesiredState == "running" && !terminalTaskState(task.State) {
				evacuated = false
				break
			}
		}
		if evacuated {
			return inventory, nil
		}
		if err := d.wait(ctx); err != nil {
			return status.Result{}, err
		}
	}
}

func (d Drainer) waitForAffectedServices(ctx context.Context, result Result, current status.Result) (status.Result, error) {
	if len(result.AffectedServices) == 0 {
		return current, nil
	}
	affected := make(map[string]struct{}, len(result.AffectedServices))
	for _, service := range result.AffectedServices {
		affected[service.ID] = struct{}{}
	}
	for {
		inventory, err := d.Client.Inspect(ctx)
		if err != nil {
			return status.Result{}, err
		}
		if err := validateLiveTarget(inventory, result); err != nil {
			return status.Result{}, err
		}
		converged := 0
		for _, service := range inventory.Services {
			if _, exists := affected[service.ID]; exists && service.Converged {
				converged++
			}
		}
		if converged == len(affected) {
			return inventory, nil
		}
		if err := d.wait(ctx); err != nil {
			return status.Result{}, err
		}
	}
}

func validateLiveTarget(inventory status.Result, result Result) error {
	if inventory.Cluster.ID != result.ClusterID {
		return fmt.Errorf("connected Swarm changed from %s to %s after drain request", result.ClusterID, inventory.Cluster.ID)
	}
	target, found := resolveTarget(inventory.Nodes, result.Target.ID)
	if !found {
		return fmt.Errorf("target node %s disappeared after drain request", result.Target.Hostname)
	}
	if result.Phase == PhaseWaitingServices && target.Availability != "drain" {
		return fmt.Errorf("target node %s availability changed to %s after evacuation", result.Target.Hostname, target.Availability)
	}
	return nil
}

func resolveTarget(nodes []status.Node, targetID string) (status.Node, bool) {
	for _, node := range nodes {
		if node.ID == targetID {
			return node, true
		}
	}
	return status.Node{}, false
}

func affectedServices(services []status.Service, preview Preview) []AffectedService {
	serviceIDs := make(map[string]struct{})
	for _, task := range preview.ReplicatedTasks {
		serviceIDs[task.ServiceID] = struct{}{}
	}
	for _, task := range preview.GlobalTasks {
		serviceIDs[task.ServiceID] = struct{}{}
	}
	affected := make([]AffectedService, 0, len(serviceIDs))
	for _, service := range services {
		if _, exists := serviceIDs[service.ID]; !exists {
			continue
		}
		affected = append(affected, AffectedService{ID: service.ID, Name: service.Name, Mode: service.Mode})
	}
	sort.Slice(affected, func(a, b int) bool {
		if affected[a].Name != affected[b].Name {
			return affected[a].Name < affected[b].Name
		}
		return affected[a].ID < affected[b].ID
	})
	return affected
}

func (d Drainer) wait(ctx context.Context) error {
	timer := time.NewTimer(d.pollInterval())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (d Drainer) reportProgress(result Result) {
	if d.Progress != nil {
		d.Progress(result)
	}
}

func (d Drainer) timeout() time.Duration {
	if d.Timeout > 0 {
		return d.Timeout
	}
	return defaultDrainTimeout
}

func (d Drainer) pollInterval() time.Duration {
	if d.PollInterval > 0 {
		return d.PollInterval
	}
	return defaultPollInterval
}
