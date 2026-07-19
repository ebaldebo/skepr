package status

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 1
const HealthSchemaVersion = 2
const ServiceDiagnosisSchemaVersion = 1

type Cluster struct {
	ID               string `json:"id"`
	LocalState       string `json:"local_state"`
	ControlAvailable bool   `json:"control_available"`
}

type Node struct {
	ID            string `json:"id"`
	Hostname      string `json:"hostname"`
	Role          string `json:"role"`
	State         string `json:"state"`
	Availability  string `json:"availability"`
	ManagerStatus string `json:"manager_status,omitempty"`
}

type Service struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Mode         string `json:"mode"`
	RunningTasks uint64 `json:"running_tasks"`
	DesiredTasks uint64 `json:"desired_tasks"`
	Converged    bool   `json:"converged"`
	ForceUpdate  uint64 `json:"-"`
}

type Task struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	ServiceID    string    `json:"-"`
	Service      string    `json:"service"`
	NodeID       string    `json:"-"`
	Node         string    `json:"node"`
	DesiredState string    `json:"desired_state"`
	State        string    `json:"state"`
	Error        string    `json:"error,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitzero"`
}

type Result struct {
	SchemaVersion  int       `json:"schema_version"`
	Endpoint       string    `json:"endpoint"`
	Cluster        Cluster   `json:"cluster"`
	Leader         string    `json:"leader,omitempty"`
	Nodes          []Node    `json:"nodes,omitempty"`
	Services       []Service `json:"services,omitempty"`
	UnhealthyTasks []Task    `json:"unhealthy_tasks,omitempty"`
	DesiredTasks   []Task    `json:"-"`
	Tasks          []Task    `json:"-"`
}

type Health string

const (
	HealthHealthy  Health = "healthy"
	HealthDegraded Health = "degraded"
)

type HealthFinding struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type HealthSummary struct {
	HealthyManagers   int `json:"healthy_managers"`
	Managers          int `json:"managers"`
	ReadyNodes        int `json:"ready_nodes"`
	ActiveNodes       int `json:"active_nodes"`
	Nodes             int `json:"nodes"`
	ConvergedServices int `json:"converged_services"`
	Services          int `json:"services"`
}

type HealthAssessment struct {
	Health   Health
	Findings []HealthFinding
	Summary  HealthSummary
}

type HealthReport struct {
	SchemaVersion  int             `json:"schema_version"`
	Health         Health          `json:"health"`
	Findings       []HealthFinding `json:"findings"`
	Summary        HealthSummary   `json:"summary"`
	Endpoint       string          `json:"endpoint"`
	Cluster        Cluster         `json:"cluster"`
	Leader         string          `json:"leader,omitempty"`
	Nodes          []Node          `json:"nodes,omitempty"`
	Services       []Service       `json:"services,omitempty"`
	UnhealthyTasks []Task          `json:"unhealthy_tasks,omitempty"`
}

type ServiceDiagnosis struct {
	SchemaVersion       int     `json:"schema_version"`
	Health              Health  `json:"health"`
	Service             Service `json:"service"`
	CurrentFailures     []Task  `json:"current_failures"`
	RecentTerminalTasks []Task  `json:"recent_terminal_tasks"`
}

func DiagnoseService(result Result, requested string) (ServiceDiagnosis, bool) {
	service, found := resolveService(result.Services, requested)
	if !found {
		return ServiceDiagnosis{}, false
	}
	diagnosis := ServiceDiagnosis{
		SchemaVersion:       ServiceDiagnosisSchemaVersion,
		Health:              HealthHealthy,
		Service:             service,
		CurrentFailures:     []Task{},
		RecentTerminalTasks: []Task{},
	}
	for _, task := range result.UnhealthyTasks {
		if task.ServiceID == service.ID {
			diagnosis.CurrentFailures = append(diagnosis.CurrentFailures, task)
		}
	}
	for _, task := range result.Tasks {
		if task.ServiceID == service.ID && task.DesiredState != "running" && terminalTaskState(task.State) {
			diagnosis.RecentTerminalTasks = append(diagnosis.RecentTerminalTasks, task)
		}
	}
	sort.Slice(diagnosis.RecentTerminalTasks, func(a, b int) bool {
		left := diagnosis.RecentTerminalTasks[a]
		right := diagnosis.RecentTerminalTasks[b]
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.After(right.UpdatedAt)
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.ID < right.ID
	})
	if len(diagnosis.RecentTerminalTasks) > 5 {
		diagnosis.RecentTerminalTasks = diagnosis.RecentTerminalTasks[:5]
	}
	if !service.Converged || len(diagnosis.CurrentFailures) > 0 {
		diagnosis.Health = HealthDegraded
	}
	return diagnosis, true
}

func terminalTaskState(state string) bool {
	switch state {
	case "complete", "shutdown", "failed", "rejected", "remove", "orphaned":
		return true
	default:
		return false
	}
}

func resolveService(services []Service, requested string) (Service, bool) {
	for _, service := range services {
		if service.ID == requested {
			return service, true
		}
	}
	for _, service := range services {
		if service.Name == requested {
			return service, true
		}
	}
	return Service{}, false
}

func BuildHealthReport(result Result, assessment HealthAssessment) HealthReport {
	return HealthReport{
		SchemaVersion:  HealthSchemaVersion,
		Health:         assessment.Health,
		Findings:       assessment.Findings,
		Summary:        assessment.Summary,
		Endpoint:       result.Endpoint,
		Cluster:        result.Cluster,
		Leader:         result.Leader,
		Nodes:          result.Nodes,
		Services:       result.Services,
		UnhealthyTasks: result.UnhealthyTasks,
	}
}

func AssessHealth(result Result) HealthAssessment {
	assessment := HealthAssessment{
		Health:   HealthHealthy,
		Findings: []HealthFinding{},
		Summary: HealthSummary{
			Nodes:    len(result.Nodes),
			Services: len(result.Services),
		},
	}
	if result.Cluster.LocalState != "active" {
		assessment.addFinding("swarm_inactive", fmt.Sprintf("Swarm state is %s, expected active", result.Cluster.LocalState))
	}
	if !result.Cluster.ControlAvailable {
		assessment.addFinding("swarm_control_unavailable", "Swarm control is unavailable")
	}
	for _, node := range result.Nodes {
		if node.State == "ready" {
			assessment.Summary.ReadyNodes++
		}
		if node.Availability == "active" {
			assessment.Summary.ActiveNodes++
		}
		if node.Role == "manager" {
			assessment.Summary.Managers++
			managerStatusHealthy := node.ManagerStatus == "leader" || node.ManagerStatus == "reachable"
			if node.State == "ready" && managerStatusHealthy {
				assessment.Summary.HealthyManagers++
			} else {
				issues := make([]string, 0, 2)
				if node.State != "ready" {
					issues = append(issues, fmt.Sprintf("state is %s, expected ready", node.State))
				}
				if !managerStatusHealthy {
					managerStatus := node.ManagerStatus
					if managerStatus == "" {
						managerStatus = "unavailable"
					}
					issues = append(issues, fmt.Sprintf("manager status is %s, expected leader or reachable", managerStatus))
				}
				assessment.addFinding("manager_unhealthy", fmt.Sprintf("manager %s is unhealthy: %s", node.Hostname, strings.Join(issues, "; ")))
			}
		} else if node.State != "ready" {
			assessment.addFinding("node_not_ready", fmt.Sprintf("node %s state is %s, expected ready", node.Hostname, node.State))
		}
	}
	for _, task := range result.UnhealthyTasks {
		message := fmt.Sprintf("task %s is %s", task.Name, task.State)
		if task.Error != "" {
			message += ": " + task.Error
		}
		assessment.addFinding("task_unhealthy", message)
	}
	for _, service := range result.Services {
		if service.Converged {
			assessment.Summary.ConvergedServices++
			continue
		}
		assessment.addFinding("service_unconverged", fmt.Sprintf("service %s has %d/%d running tasks", service.Name, service.RunningTasks, service.DesiredTasks))
	}
	return assessment
}

func (a *HealthAssessment) addFinding(code, message string) {
	a.Health = HealthDegraded
	a.Findings = append(a.Findings, HealthFinding{Code: code, Message: message})
}

type Inspector interface {
	Inspect(context.Context) (Result, error)
}

type Connection interface {
	Inspector
	io.Closer
}

type MaintenanceConnection interface {
	Connection
	UpdateNodeAvailability(context.Context, string, string) error
}

type ReconciliationConnection interface {
	Connection
	ForceUpdateService(context.Context, string) error
}

type Connector interface {
	Connect(context.Context, string) (Connection, error)
}
