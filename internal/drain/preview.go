package drain

import (
	"fmt"
	"sort"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const PreviewSchemaVersion = 1

type Destination struct {
	ID       string `json:"id"`
	Hostname string `json:"hostname"`
}

type BlockedDestination struct {
	ID       string                    `json:"id"`
	Hostname string                    `json:"hostname"`
	Blockers []status.PlacementBlocker `json:"blockers"`
}

type UnevaluatedConstraint struct {
	ServiceID  string `json:"service_id"`
	Service    string `json:"service"`
	Constraint string `json:"constraint"`
}

type ServiceImpact struct {
	ID                         string                  `json:"id"`
	Name                       string                  `json:"name"`
	TaskCount                  int                     `json:"task_count"`
	EstimatedTaskCapacity      int                     `json:"estimated_task_capacity"`
	EligibleDestinations       []Destination           `json:"eligible_destinations"`
	BlockedDestinations        []BlockedDestination    `json:"blocked_destinations"`
	StoragePortabilityWarnings []status.StorageWarning `json:"storage_portability_warnings"`
}

type Preview struct {
	SchemaVersion          int                      `json:"schema_version"`
	Endpoint               string                   `json:"endpoint"`
	RequestedNode          string                   `json:"requested_node"`
	Target                 *status.Node             `json:"target,omitempty"`
	SafeToDrain            bool                     `json:"safe_to_drain"`
	SafeToTakeOffline      bool                     `json:"safe_to_take_offline"`
	ReplicatedTasks        []preflight.WorkloadTask `json:"replicated_tasks"`
	GlobalTasks            []preflight.WorkloadTask `json:"global_tasks"`
	UnsupportedTasks       []preflight.WorkloadTask `json:"unsupported_tasks"`
	ServiceImpacts         []ServiceImpact          `json:"service_impacts"`
	UnevaluatedInputs      []string                 `json:"unevaluated_inputs"`
	UnevaluatedConstraints []UnevaluatedConstraint  `json:"unevaluated_constraints"`
	DrainFindings          []preflight.Finding      `json:"drain_findings"`
	ManagerOfflineFindings []preflight.Finding      `json:"manager_offline_findings"`
}

func BuildPreview(inventory status.Result, requestedNode string) Preview {
	check := preflight.CheckNode(inventory, requestedNode)
	preview := Preview{
		SchemaVersion:          PreviewSchemaVersion,
		Endpoint:               inventory.Endpoint,
		RequestedNode:          requestedNode,
		Target:                 check.Target,
		SafeToDrain:            true,
		ReplicatedTasks:        []preflight.WorkloadTask{},
		GlobalTasks:            []preflight.WorkloadTask{},
		UnsupportedTasks:       []preflight.WorkloadTask{},
		ServiceImpacts:         []ServiceImpact{},
		UnevaluatedInputs:      []string{},
		UnevaluatedConstraints: []UnevaluatedConstraint{},
		DrainFindings:          []preflight.Finding{},
		ManagerOfflineFindings: []preflight.Finding{},
	}
	for _, finding := range check.Findings {
		if finding.Gate == "target_not_leader" || finding.Gate == "manager_quorum" {
			preview.ManagerOfflineFindings = append(preview.ManagerOfflineFindings, finding)
			continue
		}
		preview.DrainFindings = append(preview.DrainFindings, finding)
	}
	if preview.Target == nil {
		preview.SafeToDrain = false
		return preview
	}

	servicesByID := make(map[string]status.Service, len(inventory.Services))
	servicesByName := make(map[string]status.Service, len(inventory.Services))
	for _, service := range inventory.Services {
		servicesByID[service.ID] = service
		servicesByName[service.Name] = service
	}
	replicatedServices := make(map[string]status.Service)
	unsupportedServices := make(map[string]status.Service)
	for _, task := range inventory.DesiredTasks {
		if task.DesiredState != "running" || terminalTaskState(task.State) || !assignedToTarget(task, *preview.Target) {
			continue
		}
		workloadTask := preflight.WorkloadTask{ID: task.ID, Name: task.Name, ServiceID: task.ServiceID, Service: task.Service, State: task.State}
		service, found := servicesByID[task.ServiceID]
		if !found {
			service, found = servicesByName[task.Service]
		}
		switch {
		case found && service.Mode == "replicated":
			preview.ReplicatedTasks = append(preview.ReplicatedTasks, workloadTask)
			replicatedServices[serviceKey(service)] = service
		case found && service.Mode == "global":
			preview.GlobalTasks = append(preview.GlobalTasks, workloadTask)
		default:
			preview.UnsupportedTasks = append(preview.UnsupportedTasks, workloadTask)
			if found {
				unsupportedServices[serviceKey(service)] = service
			} else {
				unsupportedServices[task.ServiceID+task.Service] = status.Service{ID: task.ServiceID, Name: task.Service}
			}
		}
	}
	sortWorkloadTasks(preview.ReplicatedTasks)
	sortWorkloadTasks(preview.GlobalTasks)
	sortWorkloadTasks(preview.UnsupportedTasks)

	for _, service := range sortedServices(replicatedServices) {
		impact, eligibility := buildServiceImpact(inventory, *preview.Target, service, countServiceTasks(preview.ReplicatedTasks, service))
		preview.ServiceImpacts = append(preview.ServiceImpacts, impact)
		for _, input := range eligibility.UnevaluatedInputs {
			if !containsString(preview.UnevaluatedInputs, input) {
				preview.UnevaluatedInputs = append(preview.UnevaluatedInputs, input)
			}
		}
		for _, constraint := range eligibility.UnevaluatedConstraints {
			preview.UnevaluatedConstraints = append(preview.UnevaluatedConstraints, UnevaluatedConstraint{
				ServiceID:  service.ID,
				Service:    service.Name,
				Constraint: constraint,
			})
		}
		eligibleCount := len(impact.EligibleDestinations)
		finding := preflight.Finding{Gate: "destination_eligible"}
		if eligibleCount == 0 {
			finding.Level = preflight.LevelBlocker
			finding.Message = fmt.Sprintf("service %s has no eligible destination based on evaluated placement inputs", service.Name)
		} else if impact.EstimatedTaskCapacity < impact.TaskCount {
			finding.Gate = "destination_capacity"
			finding.Level = preflight.LevelBlocker
			finding.Message = fmt.Sprintf("service %s has estimated destination capacity for %d of %d moving tasks based on evaluated placement inputs", service.Name, impact.EstimatedTaskCapacity, impact.TaskCount)
		} else {
			finding.Level = preflight.LevelPass
			destinationNoun := "destinations"
			if eligibleCount == 1 {
				destinationNoun = "destination"
			}
			finding.Message = fmt.Sprintf("service %s has %d eligible %s based on evaluated placement inputs", service.Name, eligibleCount, destinationNoun)
		}
		preview.DrainFindings = append(preview.DrainFindings, finding)
	}
	for _, service := range sortedServices(unsupportedServices) {
		mode := service.Mode
		if mode == "" {
			mode = "unknown"
		}
		preview.DrainFindings = append(preview.DrainFindings, preflight.Finding{
			Gate:    "service_mode_supported",
			Level:   preflight.LevelBlocker,
			Message: fmt.Sprintf("service %s uses unsupported mode %s", service.Name, mode),
		})
	}
	sort.Strings(preview.UnevaluatedInputs)
	sort.Slice(preview.UnevaluatedConstraints, func(a, b int) bool {
		if preview.UnevaluatedConstraints[a].Service != preview.UnevaluatedConstraints[b].Service {
			return preview.UnevaluatedConstraints[a].Service < preview.UnevaluatedConstraints[b].Service
		}
		return preview.UnevaluatedConstraints[a].Constraint < preview.UnevaluatedConstraints[b].Constraint
	})
	preview.SafeToDrain = !hasBlocker(preview.DrainFindings)
	preview.SafeToTakeOffline = preview.SafeToDrain && !hasBlocker(preview.ManagerOfflineFindings)
	return preview
}

func buildServiceImpact(inventory status.Result, target status.Node, service status.Service, taskCount int) (ServiceImpact, status.PlacementEligibility) {
	impact := ServiceImpact{
		ID:                         service.ID,
		Name:                       service.Name,
		TaskCount:                  taskCount,
		EligibleDestinations:       []Destination{},
		BlockedDestinations:        []BlockedDestination{},
		StoragePortabilityWarnings: []status.StorageWarning{},
	}
	diagnosis, found := status.DiagnoseService(inventory, service.ID)
	if !found {
		return impact, status.PlacementEligibility{}
	}
	impact.StoragePortabilityWarnings = diagnosis.PlacementEligibility.StoragePortabilityWarnings
	for _, node := range diagnosis.PlacementEligibility.Nodes {
		if sameNode(node.ID, node.Hostname, target) {
			continue
		}
		if node.PassesEvaluatedChecks {
			impact.EligibleDestinations = append(impact.EligibleDestinations, Destination{ID: node.ID, Hostname: node.Hostname})
			impact.EstimatedTaskCapacity += estimatedTaskCapacity(node, diagnosis.PlacementEligibility, taskCount)
			if impact.EstimatedTaskCapacity > taskCount {
				impact.EstimatedTaskCapacity = taskCount
			}
			continue
		}
		impact.BlockedDestinations = append(impact.BlockedDestinations, BlockedDestination{
			ID: node.ID, Hostname: node.Hostname, Blockers: node.Blockers,
		})
	}
	return impact, diagnosis.PlacementEligibility
}

func estimatedTaskCapacity(node status.NodePlacementEligibility, eligibility status.PlacementEligibility, movingTasks int) int {
	capacity := uint64(movingTasks)
	if eligibility.MaxReplicasPerNode > 0 {
		remaining := uint64(0)
		if node.ActiveServiceTasks < eligibility.MaxReplicasPerNode {
			remaining = eligibility.MaxReplicasPerNode - node.ActiveServiceTasks
		}
		capacity = minUint64(capacity, remaining)
	}
	if len(eligibility.RequiredHostPorts) > 0 {
		capacity = minUint64(capacity, 1)
	}
	if eligibility.RequiredResources.NanoCPUs > 0 {
		capacity = minUint64(capacity, uint64(node.Resources.Available.NanoCPUs/eligibility.RequiredResources.NanoCPUs))
	}
	if eligibility.RequiredResources.MemoryBytes > 0 {
		capacity = minUint64(capacity, uint64(node.Resources.Available.MemoryBytes/eligibility.RequiredResources.MemoryBytes))
	}
	return int(capacity)
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

func assignedToTarget(task status.Task, target status.Node) bool {
	if target.ID != "" {
		return task.NodeID == target.ID
	}
	return task.Node == target.Hostname
}

func sameNode(id, hostname string, target status.Node) bool {
	if target.ID != "" {
		return id == target.ID
	}
	return hostname == target.Hostname
}

func terminalTaskState(state string) bool {
	switch state {
	case "complete", "shutdown", "failed", "rejected", "remove", "orphaned":
		return true
	default:
		return false
	}
}

func serviceKey(service status.Service) string {
	if service.ID != "" {
		return service.ID
	}
	return service.Name
}

func sortedServices(services map[string]status.Service) []status.Service {
	result := make([]status.Service, 0, len(services))
	for _, service := range services {
		result = append(result, service)
	}
	sort.Slice(result, func(a, b int) bool {
		if result[a].Name != result[b].Name {
			return result[a].Name < result[b].Name
		}
		return result[a].ID < result[b].ID
	})
	return result
}

func sortWorkloadTasks(tasks []preflight.WorkloadTask) {
	sort.Slice(tasks, func(a, b int) bool {
		if tasks[a].Name != tasks[b].Name {
			return tasks[a].Name < tasks[b].Name
		}
		return tasks[a].ID < tasks[b].ID
	})
}

func countServiceTasks(tasks []preflight.WorkloadTask, service status.Service) int {
	count := 0
	for _, task := range tasks {
		if service.ID != "" && task.ServiceID == service.ID || service.ID == "" && task.Service == service.Name {
			count++
		}
	}
	return count
}

func hasBlocker(findings []preflight.Finding) bool {
	for _, finding := range findings {
		if finding.Level == preflight.LevelBlocker {
			return true
		}
	}
	return false
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
