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
const ServiceDiagnosisSchemaVersion = 3

type Cluster struct {
	ID               string `json:"id"`
	LocalState       string `json:"local_state"`
	ControlAvailable bool   `json:"control_available"`
}

type Node struct {
	ID            string            `json:"id"`
	Hostname      string            `json:"hostname"`
	Role          string            `json:"role"`
	State         string            `json:"state"`
	Availability  string            `json:"availability"`
	ManagerStatus string            `json:"manager_status,omitempty"`
	Labels        map[string]string `json:"-"`
}

type Service struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	Mode                 string   `json:"mode"`
	RunningTasks         uint64   `json:"running_tasks"`
	DesiredTasks         uint64   `json:"desired_tasks"`
	Converged            bool     `json:"converged"`
	ForceUpdate          uint64   `json:"-"`
	PlacementConstraints []string `json:"-"`
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
	SchemaVersion        int                  `json:"schema_version"`
	Health               Health               `json:"health"`
	Service              Service              `json:"service"`
	CurrentFailures      []Task               `json:"current_failures"`
	RecentTerminalTasks  []Task               `json:"recent_terminal_tasks"`
	PlacementEligibility PlacementEligibility `json:"placement_eligibility"`
}

type PlacementEligibility struct {
	EvaluatedInputs        []string                   `json:"evaluated_inputs"`
	UnevaluatedInputs      []string                   `json:"unevaluated_inputs"`
	EvaluatedConstraints   []string                   `json:"evaluated_constraints"`
	UnevaluatedConstraints []string                   `json:"unevaluated_constraints"`
	Nodes                  []NodePlacementEligibility `json:"nodes"`
}

type NodePlacementEligibility struct {
	ID                    string             `json:"id"`
	Hostname              string             `json:"hostname"`
	PassesEvaluatedChecks bool               `json:"passes_evaluated_checks"`
	Blockers              []PlacementBlocker `json:"blockers"`
}

type PlacementBlocker struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func DiagnoseService(result Result, requested string) (ServiceDiagnosis, bool) {
	service, found := resolveService(result.Services, requested)
	if !found {
		return ServiceDiagnosis{}, false
	}
	diagnosis := ServiceDiagnosis{
		SchemaVersion:        ServiceDiagnosisSchemaVersion,
		Health:               HealthHealthy,
		Service:              service,
		CurrentFailures:      []Task{},
		RecentTerminalTasks:  []Task{},
		PlacementEligibility: assessNodePlacement(result.Nodes, service.PlacementConstraints),
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

type placementConstraint struct {
	raw      string
	key      string
	value    string
	operator string
}

func assessNodePlacement(nodes []Node, constraints []string) PlacementEligibility {
	supportedConstraints, unevaluatedConstraints := parsePlacementConstraints(constraints)
	evaluatedConstraints := make([]string, 0, len(supportedConstraints))
	for _, constraint := range supportedConstraints {
		evaluatedConstraints = append(evaluatedConstraints, constraint.raw)
	}
	eligibility := PlacementEligibility{
		EvaluatedInputs:        []string{"node_readiness", "node_availability", "placement_constraints"},
		UnevaluatedInputs:      []string{"platform_requirements", "resource_reservations", "maximum_replicas_per_node", "host_published_port_conflicts", "storage_portability"},
		EvaluatedConstraints:   evaluatedConstraints,
		UnevaluatedConstraints: unevaluatedConstraints,
		Nodes:                  make([]NodePlacementEligibility, 0, len(nodes)),
	}
	for _, node := range nodes {
		result := NodePlacementEligibility{
			ID:       node.ID,
			Hostname: node.Hostname,
			Blockers: []PlacementBlocker{},
		}
		if node.State != "ready" {
			result.Blockers = append(result.Blockers, PlacementBlocker{
				Code:    "node_not_ready",
				Message: fmt.Sprintf("state is %s", node.State),
			})
		}
		if node.Availability != "active" {
			result.Blockers = append(result.Blockers, PlacementBlocker{
				Code:    "node_not_active",
				Message: fmt.Sprintf("availability is %s", node.Availability),
			})
		}
		for _, constraint := range supportedConstraints {
			if !constraint.matches(node) {
				result.Blockers = append(result.Blockers, PlacementBlocker{
					Code:    "constraint_mismatch",
					Message: fmt.Sprintf("constraint %s does not match", constraint.raw),
				})
			}
		}
		result.PassesEvaluatedChecks = len(result.Blockers) == 0
		eligibility.Nodes = append(eligibility.Nodes, result)
	}
	sort.Slice(eligibility.Nodes, func(a, b int) bool {
		if eligibility.Nodes[a].Hostname != eligibility.Nodes[b].Hostname {
			return eligibility.Nodes[a].Hostname < eligibility.Nodes[b].Hostname
		}
		return eligibility.Nodes[a].ID < eligibility.Nodes[b].ID
	})
	return eligibility
}

func parsePlacementConstraints(rawConstraints []string) ([]placementConstraint, []string) {
	constraints := make([]placementConstraint, 0, len(rawConstraints))
	unevaluated := make([]string, 0)
	for _, raw := range rawConstraints {
		constraint, supported := parsePlacementConstraint(raw)
		if supported {
			constraints = append(constraints, constraint)
		} else {
			unevaluated = append(unevaluated, raw)
		}
	}
	return constraints, unevaluated
}

func parsePlacementConstraint(raw string) (placementConstraint, bool) {
	operator := "=="
	operatorIndex := strings.Index(raw, operator)
	if operatorIndex < 0 {
		operator = "!="
		operatorIndex = strings.Index(raw, operator)
	}
	if operatorIndex < 0 {
		return placementConstraint{}, false
	}
	key := strings.TrimSpace(raw[:operatorIndex])
	value := strings.TrimSpace(raw[operatorIndex+len(operator):])
	if value == "" {
		return placementConstraint{}, false
	}
	supported := key == "node.id" || key == "node.hostname" || key == "node.role" || strings.HasPrefix(key, "node.labels.") && len(key) > len("node.labels.")
	if !supported {
		return placementConstraint{}, false
	}
	return placementConstraint{raw: raw, key: key, value: value, operator: operator}, true
}

func (c placementConstraint) matches(node Node) bool {
	actual := ""
	found := true
	switch {
	case c.key == "node.id":
		actual = node.ID
	case c.key == "node.hostname":
		actual = node.Hostname
	case c.key == "node.role":
		actual = node.Role
	case strings.HasPrefix(c.key, "node.labels."):
		actual, found = node.Labels[strings.TrimPrefix(c.key, "node.labels.")]
	}
	equal := found && strings.EqualFold(actual, c.value)
	if c.operator == "!=" {
		return !equal
	}
	return equal
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
