package activate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const ResultSchemaVersion = 1

const (
	PhaseActivating = "activating"
	PhaseVerifying  = "verifying"
	PhaseActive     = "active"
)

const (
	defaultTimeout      = 5 * time.Minute
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

type SafetyReport struct {
	SchemaVersion int                 `json:"schema_version"`
	Endpoint      string              `json:"endpoint"`
	RequestedNode string              `json:"requested_node"`
	Target        *status.Node        `json:"target,omitempty"`
	Safe          bool                `json:"safe"`
	Findings      []preflight.Finding `json:"findings"`
}

type Result struct {
	SchemaVersion     int         `json:"schema_version"`
	Endpoint          string      `json:"endpoint"`
	ClusterID         string      `json:"cluster_id"`
	Target            status.Node `json:"target"`
	Phase             string      `json:"phase"`
	MutationOccurred  bool        `json:"mutation_occurred"`
	Availability      string      `json:"availability"`
	HealthyManagers   int         `json:"healthy_managers"`
	TotalManagers     int         `json:"total_managers"`
	ConvergedServices int         `json:"converged_services"`
	TotalServices     int         `json:"total_services"`
}

type Activator struct {
	Client       Client
	Guard        Guard
	Timeout      time.Duration
	PollInterval time.Duration
	Progress     func(Result)
}

type SafetyError struct {
	Report SafetyReport
}

func (e *SafetyError) Error() string {
	return fmt.Sprintf("node activation safety checks blocked target %s", e.Report.RequestedNode)
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
	return fmt.Sprintf("node activation failed in phase %s: %v", e.Result.Phase, e.Err)
}

func (e *MutationError) Unwrap() error {
	return e.Err
}

func (a Activator) Activate(ctx context.Context, requestedNode string) (Result, error) {
	initialInventory, err := a.Client.Inspect(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("inspect Swarm before node activation: %w", err)
	}
	initialReport := BuildSafetyReport(initialInventory, requestedNode, "drain")
	if !initialReport.Safe {
		return Result{}, &SafetyError{Report: initialReport}
	}
	if a.Guard == nil {
		return Result{}, fmt.Errorf("node activation operation guard is required")
	}
	release, err := a.Guard.AcquireClusterLock(initialInventory.Cluster.ID)
	if err != nil {
		return Result{}, &ValidationError{Message: fmt.Sprintf("acquire node activation lock: %v", err)}
	}
	defer func() { _ = release() }()
	if err := a.Guard.EnsureNoActiveOperation(initialInventory.Cluster.ID); err != nil {
		return Result{}, &ValidationError{Message: err.Error()}
	}

	inventory, err := a.Client.Inspect(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("repeat Swarm inspection before node activation: %w", err)
	}
	report := BuildSafetyReport(inventory, requestedNode, "drain")
	if !report.Safe {
		return Result{}, &SafetyError{Report: report}
	}
	if inventory.Cluster.ID != initialInventory.Cluster.ID {
		return Result{}, &ValidationError{Message: fmt.Sprintf("connected Swarm changed from %s to %s before node activation", initialInventory.Cluster.ID, inventory.Cluster.ID)}
	}
	if report.Target == nil || initialReport.Target == nil || report.Target.ID != initialReport.Target.ID {
		return Result{}, &ValidationError{Message: fmt.Sprintf("target node %s changed before node activation", requestedNode)}
	}

	result := Result{
		SchemaVersion:     ResultSchemaVersion,
		Endpoint:          inventory.Endpoint,
		ClusterID:         inventory.Cluster.ID,
		Target:            *report.Target,
		Phase:             PhaseActivating,
		MutationOccurred:  true,
		Availability:      report.Target.Availability,
		HealthyManagers:   countHealthyManagers(inventory.Nodes, report.Target.ID, "drain"),
		TotalManagers:     countManagers(inventory.Nodes),
		ConvergedServices: countConvergedServices(inventory.Services),
		TotalServices:     len(inventory.Services),
	}
	a.reportProgress(result)
	mutationCtx, cancel := context.WithTimeout(ctx, a.timeout())
	defer cancel()
	if err := a.Client.UpdateNodeAvailability(mutationCtx, result.Target.ID, "active"); err != nil {
		return result, &MutationError{Result: result, Err: fmt.Errorf("request activation for target node %s: %w", result.Target.Hostname, err)}
	}

	result.Phase = PhaseVerifying
	a.reportProgress(result)
	finalInventory, err := a.waitForHealthyCluster(mutationCtx, result)
	if err != nil {
		return result, &MutationError{Result: result, Err: fmt.Errorf("verify cluster after activating target: %w", err)}
	}
	finalReport := BuildSafetyReport(finalInventory, result.Target.ID, "active")
	result.Target = *finalReport.Target
	result.Availability = result.Target.Availability
	result.HealthyManagers = countHealthyManagers(finalInventory.Nodes, result.Target.ID, "active")
	result.TotalManagers = countManagers(finalInventory.Nodes)
	result.ConvergedServices = countConvergedServices(finalInventory.Services)
	result.TotalServices = len(finalInventory.Services)
	result.Phase = PhaseActive
	a.reportProgress(result)
	return result, nil
}

func BuildSafetyReport(inventory status.Result, requestedNode, expectedAvailability string) SafetyReport {
	report := SafetyReport{
		SchemaVersion: ResultSchemaVersion,
		Endpoint:      inventory.Endpoint,
		RequestedNode: requestedNode,
		Safe:          true,
		Findings:      []preflight.Finding{},
	}
	for _, node := range inventory.Nodes {
		if node.ID == requestedNode || node.Hostname == requestedNode {
			target := node
			report.Target = &target
			break
		}
	}
	if report.Target == nil {
		report.addBlocker("target_exists", fmt.Sprintf("target node %s was not found", requestedNode))
	} else {
		report.addPass("target_exists", fmt.Sprintf("target node %s exists with role %s", report.Target.Hostname, report.Target.Role))
		if report.Target.State == "ready" {
			report.addPass("target_ready", fmt.Sprintf("target node %s is ready", report.Target.Hostname))
		} else {
			report.addBlocker("target_ready", fmt.Sprintf("target node %s state is %s, expected ready", report.Target.Hostname, report.Target.State))
		}
		if report.Target.Availability == expectedAvailability {
			report.addPass("target_availability", fmt.Sprintf("target node %s is %s", report.Target.Hostname, expectedAvailability))
		} else {
			report.addBlocker("target_availability", fmt.Sprintf("target node %s availability is %s, expected %s", report.Target.Hostname, report.Target.Availability, expectedAvailability))
		}
	}
	if inventory.Cluster.LocalState == "active" {
		report.addPass("swarm_active", "connected Docker endpoint is part of an active Swarm")
	} else {
		report.addBlocker("swarm_active", fmt.Sprintf("connected Docker endpoint Swarm state is %s, expected active", inventory.Cluster.LocalState))
	}
	if inventory.Cluster.ControlAvailable {
		report.addPass("swarm_control_available", "connected Docker endpoint provides Swarm manager control")
	} else {
		report.addBlocker("swarm_control_available", "connected Docker endpoint does not provide Swarm manager control")
	}

	targetID := ""
	if report.Target != nil {
		targetID = report.Target.ID
	}
	totalManagers := 0
	healthyManagers := 0
	for _, node := range inventory.Nodes {
		if node.Role != "manager" {
			continue
		}
		totalManagers++
		expectedManagerAvailability := "active"
		if node.ID == targetID {
			expectedManagerAvailability = expectedAvailability
		}
		issues := managerHealthIssues(node, expectedManagerAvailability)
		if len(issues) == 0 {
			healthyManagers++
			report.addPass("manager_healthy", fmt.Sprintf("Swarm manager %s is healthy (ready, %s and %s)", node.Hostname, expectedManagerAvailability, node.ManagerStatus))
		} else {
			report.addBlocker("manager_healthy", fmt.Sprintf("Swarm manager %s is unhealthy: %s", node.Hostname, strings.Join(issues, "; ")))
		}
	}
	if totalManagers == 0 {
		report.addBlocker("manager_quorum", "connected Swarm has no manager membership")
	} else {
		required := totalManagers/2 + 1
		message := fmt.Sprintf("%d healthy managers satisfy quorum requirement %d of %d", healthyManagers, required, totalManagers)
		if healthyManagers >= required {
			report.addPass("manager_quorum", message)
		} else {
			report.addBlocker("manager_quorum", message)
		}
	}

	converged := countConvergedServices(inventory.Services)
	if converged == len(inventory.Services) {
		report.addPass("service_converged", fmt.Sprintf("all %d Swarm services are converged", len(inventory.Services)))
	} else {
		for _, service := range inventory.Services {
			if !service.Converged {
				report.addBlocker("service_converged", fmt.Sprintf("Swarm service %s has %d of %d running tasks", service.Name, service.RunningTasks, service.DesiredTasks))
			}
		}
	}
	return report
}

func (a Activator) waitForHealthyCluster(ctx context.Context, result Result) (status.Result, error) {
	var lastErr error
	for {
		inventory, err := a.Client.Inspect(ctx)
		if err == nil {
			if inventory.Cluster.ID != result.ClusterID {
				return status.Result{}, fmt.Errorf("connected Swarm changed from %s to %s after activation request", result.ClusterID, inventory.Cluster.ID)
			}
			report := BuildSafetyReport(inventory, result.Target.ID, "active")
			if report.Target == nil {
				return status.Result{}, fmt.Errorf("target node %s disappeared after activation request", result.Target.Hostname)
			}
			if report.Safe {
				return inventory, nil
			}
			lastErr = fmt.Errorf("cluster has not returned to a healthy active state")
		} else {
			lastErr = err
		}
		if err := a.wait(ctx); err != nil {
			if lastErr != nil {
				return status.Result{}, fmt.Errorf("%v: %w", lastErr, err)
			}
			return status.Result{}, err
		}
	}
}

func managerHealthIssues(node status.Node, expectedAvailability string) []string {
	var issues []string
	if node.State != "ready" {
		issues = append(issues, fmt.Sprintf("state is %s, expected ready", node.State))
	}
	if node.Availability != expectedAvailability {
		issues = append(issues, fmt.Sprintf("availability is %s, expected %s", node.Availability, expectedAvailability))
	}
	if node.ManagerStatus != "leader" && node.ManagerStatus != "reachable" {
		managerStatus := node.ManagerStatus
		if managerStatus == "" {
			managerStatus = "unavailable"
		}
		issues = append(issues, fmt.Sprintf("manager status is %s, expected leader or reachable", managerStatus))
	}
	return issues
}

func countManagers(nodes []status.Node) int {
	count := 0
	for _, node := range nodes {
		if node.Role == "manager" {
			count++
		}
	}
	return count
}

func countHealthyManagers(nodes []status.Node, targetID, targetAvailability string) int {
	count := 0
	for _, node := range nodes {
		if node.Role != "manager" {
			continue
		}
		expectedAvailability := "active"
		if node.ID == targetID {
			expectedAvailability = targetAvailability
		}
		if len(managerHealthIssues(node, expectedAvailability)) == 0 {
			count++
		}
	}
	return count
}

func countConvergedServices(services []status.Service) int {
	count := 0
	for _, service := range services {
		if service.Converged {
			count++
		}
	}
	return count
}

func (r *SafetyReport) addPass(gate, message string) {
	r.Findings = append(r.Findings, preflight.Finding{Gate: gate, Level: preflight.LevelPass, Message: message})
}

func (r *SafetyReport) addBlocker(gate, message string) {
	r.Safe = false
	r.Findings = append(r.Findings, preflight.Finding{Gate: gate, Level: preflight.LevelBlocker, Message: message})
}

func (a Activator) wait(ctx context.Context) error {
	timer := time.NewTimer(a.pollInterval())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (a Activator) reportProgress(result Result) {
	if a.Progress != nil {
		a.Progress(result)
	}
}

func (a Activator) timeout() time.Duration {
	if a.Timeout > 0 {
		return a.Timeout
	}
	return defaultTimeout
}

func (a Activator) pollInterval() time.Duration {
	if a.PollInterval > 0 {
		return a.PollInterval
	}
	return defaultPollInterval
}
