package rebalance

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ebaldebo/skepr/internal/status"
)

const SchemaVersion = 2

const (
	StateOpportunity   = "opportunity"
	StateNoOpportunity = "no-opportunity"
	StateConstrained   = "constrained"
	StateNotAssessed   = "not-assessed"
)

type Summary struct {
	ReplicatedServices             int `json:"replicated_services"`
	AssessedServices               int `json:"assessed_services"`
	Opportunities                  int `json:"opportunities"`
	ConstrainedServices            int `json:"constrained_services"`
	NotAssessedServices            int `json:"not_assessed_services"`
	ActiveTasks                    int `json:"active_tasks"`
	TasksWithoutCPUReservations    int `json:"tasks_without_cpu_reservations"`
	TasksWithoutMemoryReservations int `json:"tasks_without_memory_reservations"`
}

type NodeTaskCount struct {
	ID       string `json:"id"`
	Hostname string `json:"hostname"`
	Tasks    uint64 `json:"tasks"`
}

type ServiceAssessment struct {
	ID                        string                  `json:"id"`
	Name                      string                  `json:"name"`
	Mode                      string                  `json:"mode"`
	Replicas                  uint64                  `json:"replicas"`
	State                     string                  `json:"state"`
	Reason                    string                  `json:"reason,omitempty"`
	Skew                      uint64                  `json:"skew"`
	Distribution              []NodeTaskCount         `json:"distribution"`
	OverloadedNodes           []NodeTaskCount         `json:"overloaded_nodes"`
	KnownEligibleDestinations []NodeTaskCount         `json:"known_eligible_destinations"`
	StorageWarnings           []status.StorageWarning `json:"storage_warnings"`
	UnevaluatedInputs         []string                `json:"unevaluated_inputs"`
	UnevaluatedConstraints    []string                `json:"unevaluated_constraints"`
}

type TaskReference struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Service string `json:"service"`
}

type NodeReservation struct {
	ID                             string               `json:"id"`
	Hostname                       string               `json:"hostname"`
	ActiveTasks                    int                  `json:"active_tasks"`
	Resources                      status.NodeResources `json:"resources"`
	TasksWithoutCPUReservations    []TaskReference      `json:"tasks_without_cpu_reservations"`
	TasksWithoutMemoryReservations []TaskReference      `json:"tasks_without_memory_reservations"`
}

type Report struct {
	SchemaVersion    int                 `json:"schema_version"`
	Endpoint         string              `json:"endpoint"`
	ClusterID        string              `json:"cluster_id"`
	Summary          Summary             `json:"summary"`
	Services         []ServiceAssessment `json:"services"`
	NodeReservations []NodeReservation   `json:"node_reservations"`
}

func BuildReport(inventory status.Result) Report {
	report := Report{
		SchemaVersion:    SchemaVersion,
		Endpoint:         inventory.Endpoint,
		ClusterID:        inventory.Cluster.ID,
		Services:         []ServiceAssessment{},
		NodeReservations: buildNodeReservations(inventory),
	}
	for _, node := range report.NodeReservations {
		report.Summary.ActiveTasks += node.ActiveTasks
		report.Summary.TasksWithoutCPUReservations += len(node.TasksWithoutCPUReservations)
		report.Summary.TasksWithoutMemoryReservations += len(node.TasksWithoutMemoryReservations)
	}
	for _, service := range inventory.Services {
		assessment := assessService(inventory, service)
		report.Services = append(report.Services, assessment)
		if service.Mode == "replicated" {
			report.Summary.ReplicatedServices++
		}
		switch assessment.State {
		case StateOpportunity:
			report.Summary.AssessedServices++
			report.Summary.Opportunities++
		case StateNoOpportunity:
			report.Summary.AssessedServices++
		case StateConstrained:
			report.Summary.AssessedServices++
			report.Summary.ConstrainedServices++
		case StateNotAssessed:
			report.Summary.NotAssessedServices++
		}
	}
	sort.Slice(report.Services, func(a, b int) bool {
		if report.Services[a].Name != report.Services[b].Name {
			return report.Services[a].Name < report.Services[b].Name
		}
		return report.Services[a].ID < report.Services[b].ID
	})
	return report
}

func buildNodeReservations(inventory status.Result) []NodeReservation {
	reservations := make([]NodeReservation, 0, len(inventory.Nodes))
	for _, node := range inventory.Nodes {
		reservation := NodeReservation{
			ID:                             node.ID,
			Hostname:                       node.Hostname,
			Resources:                      status.NodeResources{Capacity: node.Resources, Available: node.Resources},
			TasksWithoutCPUReservations:    []TaskReference{},
			TasksWithoutMemoryReservations: []TaskReference{},
		}
		for _, task := range inventory.Tasks {
			if task.NodeID != node.ID || !activeTaskState(task.State) {
				continue
			}
			reservation.ActiveTasks++
			reservation.Resources.Reserved.NanoCPUs += task.Reservations.NanoCPUs
			reservation.Resources.Reserved.MemoryBytes += task.Reservations.MemoryBytes
			reference := TaskReference{ID: task.ID, Name: task.Name, Service: task.Service}
			if task.Reservations.NanoCPUs == 0 {
				reservation.TasksWithoutCPUReservations = append(reservation.TasksWithoutCPUReservations, reference)
			}
			if task.Reservations.MemoryBytes == 0 {
				reservation.TasksWithoutMemoryReservations = append(reservation.TasksWithoutMemoryReservations, reference)
			}
		}
		reservation.Resources.Available.NanoCPUs = max(reservation.Resources.Capacity.NanoCPUs-reservation.Resources.Reserved.NanoCPUs, 0)
		reservation.Resources.Available.MemoryBytes = max(reservation.Resources.Capacity.MemoryBytes-reservation.Resources.Reserved.MemoryBytes, 0)
		sortTaskReferences(reservation.TasksWithoutCPUReservations)
		sortTaskReferences(reservation.TasksWithoutMemoryReservations)
		reservations = append(reservations, reservation)
	}
	sort.Slice(reservations, func(a, b int) bool {
		if reservations[a].Hostname != reservations[b].Hostname {
			return reservations[a].Hostname < reservations[b].Hostname
		}
		return reservations[a].ID < reservations[b].ID
	})
	return reservations
}

func sortTaskReferences(tasks []TaskReference) {
	sort.Slice(tasks, func(a, b int) bool {
		if tasks[a].Name != tasks[b].Name {
			return tasks[a].Name < tasks[b].Name
		}
		return tasks[a].ID < tasks[b].ID
	})
}

func assessService(inventory status.Result, service status.Service) ServiceAssessment {
	assessment := ServiceAssessment{
		ID:                        service.ID,
		Name:                      service.Name,
		Mode:                      service.Mode,
		Replicas:                  service.DesiredTasks,
		State:                     StateNotAssessed,
		Distribution:              []NodeTaskCount{},
		OverloadedNodes:           []NodeTaskCount{},
		KnownEligibleDestinations: []NodeTaskCount{},
		StorageWarnings:           []status.StorageWarning{},
		UnevaluatedInputs:         []string{},
		UnevaluatedConstraints:    []string{},
	}
	if service.Mode != "replicated" {
		assessment.Reason = fmt.Sprintf("mode %s is not replicated", service.Mode)
		return assessment
	}
	if !service.Converged {
		assessment.Reason = fmt.Sprintf("service is unconverged at %d/%d running tasks", service.RunningTasks, service.DesiredTasks)
		return assessment
	}
	if len(service.PlacementPreferences) > 0 {
		assessment.Reason = "placement preferences are configured: " + strings.Join(service.PlacementPreferences, ", ")
		return assessment
	}

	diagnosis, found := status.DiagnoseService(inventory, service.ID)
	if !found {
		assessment.Reason = "service placement state is unavailable"
		return assessment
	}
	assessment.StorageWarnings = diagnosis.PlacementEligibility.StoragePortabilityWarnings
	assessment.UnevaluatedInputs = diagnosis.PlacementEligibility.UnevaluatedInputs
	assessment.UnevaluatedConstraints = diagnosis.PlacementEligibility.UnevaluatedConstraints
	if len(assessment.UnevaluatedConstraints) > 0 {
		assessment.Reason = "placement constraints are not fully evaluated: " + strings.Join(assessment.UnevaluatedConstraints, ", ")
		return assessment
	}

	counts := taskCountsByNode(inventory.DesiredTasks, service.ID)
	considered := make(map[string]status.NodePlacementEligibility)
	for _, node := range diagnosis.PlacementEligibility.Nodes {
		if counts[node.ID] > 0 || node.PassesEvaluatedChecks {
			considered[node.ID] = node
		}
	}
	for nodeID := range counts {
		if _, exists := considered[nodeID]; exists {
			continue
		}
		considered[nodeID] = status.NodePlacementEligibility{ID: nodeID, Hostname: nodeID}
	}
	for _, node := range considered {
		hostname := node.Hostname
		if hostname == "" {
			hostname = node.ID
		}
		assessment.Distribution = append(assessment.Distribution, NodeTaskCount{ID: node.ID, Hostname: hostname, Tasks: counts[node.ID]})
	}
	sortNodeTaskCounts(assessment.Distribution)
	if len(assessment.Distribution) == 0 {
		assessment.State = StateNoOpportunity
		return assessment
	}

	minimum := assessment.Distribution[0].Tasks
	maximum := minimum
	for _, node := range assessment.Distribution[1:] {
		minimum = min(minimum, node.Tasks)
		maximum = max(maximum, node.Tasks)
	}
	assessment.Skew = maximum - minimum
	for _, node := range assessment.Distribution {
		if maximum > 0 && node.Tasks == maximum {
			assessment.OverloadedNodes = append(assessment.OverloadedNodes, node)
		}
		placement := considered[node.ID]
		if placement.PassesEvaluatedChecks && maximum >= node.Tasks+2 {
			assessment.KnownEligibleDestinations = append(assessment.KnownEligibleDestinations, node)
		}
	}
	if len(assessment.KnownEligibleDestinations) > 0 {
		assessment.State = StateOpportunity
	} else if assessment.Skew >= 2 {
		assessment.State = StateConstrained
		assessment.Reason = "no lower-count node passes evaluated placement checks"
	} else {
		assessment.State = StateNoOpportunity
	}
	return assessment
}

func taskCountsByNode(tasks []status.Task, serviceID string) map[string]uint64 {
	counts := make(map[string]uint64)
	for _, task := range tasks {
		if task.ServiceID == serviceID && task.DesiredState == "running" && task.NodeID != "" && activeTaskState(task.State) {
			counts[task.NodeID]++
		}
	}
	return counts
}

func activeTaskState(state string) bool {
	switch state {
	case "failed", "rejected", "orphaned", "complete", "completed", "shutdown", "remove", "removed":
		return false
	default:
		return true
	}
}

func sortNodeTaskCounts(nodes []NodeTaskCount) {
	sort.Slice(nodes, func(a, b int) bool {
		if nodes[a].Hostname != nodes[b].Hostname {
			return nodes[a].Hostname < nodes[b].Hostname
		}
		return nodes[a].ID < nodes[b].ID
	})
}
