package status

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 1
const HealthSchemaVersion = 2
const ServiceDiagnosisSchemaVersion = 8

type HostPort struct {
	Protocol      string `json:"protocol"`
	PublishedPort uint32 `json:"published_port"`
}

type StorageMount struct {
	Type      string
	Source    string
	Target    string
	NodeLocal bool
}

type StorageWarning struct {
	Code    string `json:"code"`
	Source  string `json:"source"`
	Target  string `json:"target"`
	Message string `json:"message"`
}

func (p HostPort) String() string {
	return fmt.Sprintf("%d/%s", p.PublishedPort, p.Protocol)
}

type Resources struct {
	NanoCPUs    int64 `json:"nano_cpus"`
	MemoryBytes int64 `json:"memory_bytes"`
}

func (r Resources) String() string {
	parts := make([]string, 0, 2)
	if r.NanoCPUs > 0 {
		parts = append(parts, formatCPUs(r.NanoCPUs))
	}
	if r.MemoryBytes > 0 {
		parts = append(parts, formatMemory(r.MemoryBytes)+" memory")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

type Platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

func (p Platform) String() string {
	switch {
	case p.OS == "" && p.Architecture == "":
		return "unknown"
	case p.OS == "":
		return p.Architecture
	case p.Architecture == "":
		return p.OS
	default:
		return p.OS + "/" + p.Architecture
	}
}

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
	Platform      Platform          `json:"-"`
	Resources     Resources         `json:"-"`
}

type Service struct {
	ID                   string         `json:"id"`
	Name                 string         `json:"name"`
	Mode                 string         `json:"mode"`
	RunningTasks         uint64         `json:"running_tasks"`
	DesiredTasks         uint64         `json:"desired_tasks"`
	Converged            bool           `json:"converged"`
	ForceUpdate          uint64         `json:"-"`
	PlacementConstraints []string       `json:"-"`
	PlacementPreferences []string       `json:"-"`
	RequiredPlatforms    []Platform     `json:"-"`
	Reservations         Resources      `json:"-"`
	MaxReplicasPerNode   uint64         `json:"-"`
	HostPorts            []HostPort     `json:"-"`
	StorageMounts        []StorageMount `json:"-"`
}

type Task struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	ServiceID    string     `json:"-"`
	Service      string     `json:"service"`
	NodeID       string     `json:"-"`
	Node         string     `json:"node"`
	DesiredState string     `json:"desired_state"`
	State        string     `json:"state"`
	Error        string     `json:"error,omitempty"`
	UpdatedAt    time.Time  `json:"updated_at,omitzero"`
	Reservations Resources  `json:"-"`
	HostPorts    []HostPort `json:"-"`
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
	EvaluatedInputs            []string                   `json:"evaluated_inputs"`
	UnevaluatedInputs          []string                   `json:"unevaluated_inputs"`
	EvaluatedConstraints       []string                   `json:"evaluated_constraints"`
	UnevaluatedConstraints     []string                   `json:"unevaluated_constraints"`
	RequiredPlatforms          []Platform                 `json:"required_platforms"`
	RequiredResources          Resources                  `json:"required_resources"`
	MaxReplicasPerNode         uint64                     `json:"max_replicas_per_node"`
	RequiredHostPorts          []HostPort                 `json:"required_host_ports"`
	StoragePortabilityWarnings []StorageWarning           `json:"storage_portability_warnings"`
	Nodes                      []NodePlacementEligibility `json:"nodes"`
}

type NodePlacementEligibility struct {
	ID                    string             `json:"id"`
	Hostname              string             `json:"hostname"`
	PassesEvaluatedChecks bool               `json:"passes_evaluated_checks"`
	Blockers              []PlacementBlocker `json:"blockers"`
	Resources             NodeResources      `json:"resources,omitzero"`
	ActiveServiceTasks    uint64             `json:"active_service_tasks"`
	UsedHostPorts         []HostPort         `json:"used_host_ports"`
}

type NodeResources struct {
	Capacity  Resources `json:"capacity"`
	Reserved  Resources `json:"reserved"`
	Available Resources `json:"available"`
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
		PlacementEligibility: assessNodePlacement(result, service),
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

func assessNodePlacement(inventory Result, service Service) PlacementEligibility {
	supportedConstraints, unevaluatedConstraints := parsePlacementConstraints(service.PlacementConstraints)
	requiredHostPorts := canonicalHostPorts(service.HostPorts)
	storageWarnings := storagePortabilityWarnings(service.StorageMounts)
	evaluatedConstraints := make([]string, 0, len(supportedConstraints))
	for _, constraint := range supportedConstraints {
		evaluatedConstraints = append(evaluatedConstraints, constraint.raw)
	}
	eligibility := PlacementEligibility{
		EvaluatedInputs:            []string{"node_readiness", "node_availability", "placement_constraints", "platform_requirements", "cpu_memory_reservations", "maximum_replicas_per_node", "host_published_port_conflicts", "storage_portability_warnings"},
		UnevaluatedInputs:          []string{"generic_resources"},
		EvaluatedConstraints:       evaluatedConstraints,
		UnevaluatedConstraints:     unevaluatedConstraints,
		RequiredPlatforms:          append([]Platform{}, service.RequiredPlatforms...),
		RequiredResources:          service.Reservations,
		MaxReplicasPerNode:         service.MaxReplicasPerNode,
		RequiredHostPorts:          requiredHostPorts,
		StoragePortabilityWarnings: storageWarnings,
		Nodes:                      make([]NodePlacementEligibility, 0, len(inventory.Nodes)),
	}
	reservedByNode := reservedResourcesByNode(inventory.Tasks)
	activeServiceTasksByNode := activeTasksByNode(inventory.Tasks, service.ID)
	usedHostPortsByNode := usedHostPortsByNode(inventory.Tasks)
	for _, node := range inventory.Nodes {
		reserved := reservedByNode[node.ID]
		available := availableResources(node.Resources, reserved)
		activeServiceTasks := activeServiceTasksByNode[node.ID]
		usedHostPorts := usedHostPortsByNode[node.ID]
		result := NodePlacementEligibility{
			ID:                 node.ID,
			Hostname:           node.Hostname,
			Blockers:           []PlacementBlocker{},
			ActiveServiceTasks: activeServiceTasks,
			UsedHostPorts:      append([]HostPort{}, usedHostPorts...),
			Resources: NodeResources{
				Capacity:  node.Resources,
				Reserved:  reserved,
				Available: available,
			},
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
		if len(service.RequiredPlatforms) > 0 && !matchesRequiredPlatform(node.Platform, service.RequiredPlatforms) {
			platforms := make([]string, 0, len(service.RequiredPlatforms))
			for _, platform := range service.RequiredPlatforms {
				platforms = append(platforms, platform.String())
			}
			result.Blockers = append(result.Blockers, PlacementBlocker{
				Code:    "platform_mismatch",
				Message: fmt.Sprintf("platform %s does not match required %s", node.Platform, strings.Join(platforms, " or ")),
			})
		}
		if service.Reservations.NanoCPUs > available.NanoCPUs {
			result.Blockers = append(result.Blockers, PlacementBlocker{
				Code:    "insufficient_cpu",
				Message: fmt.Sprintf("requires %s, %s available", formatCPUs(service.Reservations.NanoCPUs), formatCPUs(available.NanoCPUs)),
			})
		}
		if service.Reservations.MemoryBytes > available.MemoryBytes {
			result.Blockers = append(result.Blockers, PlacementBlocker{
				Code:    "insufficient_memory",
				Message: fmt.Sprintf("requires %s memory, %s available", formatMemory(service.Reservations.MemoryBytes), formatMemory(available.MemoryBytes)),
			})
		}
		if service.MaxReplicasPerNode > 0 && activeServiceTasks >= service.MaxReplicasPerNode {
			taskWord := "tasks"
			if activeServiceTasks == 1 {
				taskWord = "task"
			}
			result.Blockers = append(result.Blockers, PlacementBlocker{
				Code:    "max_replicas_per_node",
				Message: fmt.Sprintf("service already has %d active %s, limit is %d", activeServiceTasks, taskWord, service.MaxReplicasPerNode),
			})
		}
		for _, requiredPort := range requiredHostPorts {
			if slices.Contains(usedHostPorts, requiredPort) {
				result.Blockers = append(result.Blockers, PlacementBlocker{
					Code:    "host_port_conflict",
					Message: fmt.Sprintf("host port %s is already in use", requiredPort),
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

func storagePortabilityWarnings(mounts []StorageMount) []StorageWarning {
	warnings := make([]StorageWarning, 0, len(mounts))
	for _, mount := range mounts {
		var warning StorageWarning
		switch {
		case mount.Type == "bind":
			warning = StorageWarning{
				Code:    "bind_mount",
				Source:  mount.Source,
				Target:  mount.Target,
				Message: fmt.Sprintf("bind mount %s -> %s may not be portable across nodes", mount.Source, mount.Target),
			}
		case mount.Type == "volume" && mount.NodeLocal:
			message := fmt.Sprintf("volume %s -> %s uses node-local storage", mount.Source, mount.Target)
			if mount.Source == "" {
				message = fmt.Sprintf("anonymous volume -> %s uses node-local storage", mount.Target)
			}
			warning = StorageWarning{
				Code:    "node_local_volume",
				Source:  mount.Source,
				Target:  mount.Target,
				Message: message,
			}
		default:
			continue
		}
		if !slices.Contains(warnings, warning) {
			warnings = append(warnings, warning)
		}
	}
	sort.Slice(warnings, func(a, b int) bool {
		if warnings[a].Target != warnings[b].Target {
			return warnings[a].Target < warnings[b].Target
		}
		if warnings[a].Source != warnings[b].Source {
			return warnings[a].Source < warnings[b].Source
		}
		return warnings[a].Code < warnings[b].Code
	})
	return warnings
}

func canonicalHostPorts(ports []HostPort) []HostPort {
	canonical := make([]HostPort, 0, len(ports))
	for _, port := range ports {
		if !slices.Contains(canonical, port) {
			canonical = append(canonical, port)
		}
	}
	sort.Slice(canonical, func(a, b int) bool {
		if canonical[a].PublishedPort != canonical[b].PublishedPort {
			return canonical[a].PublishedPort < canonical[b].PublishedPort
		}
		return canonical[a].Protocol < canonical[b].Protocol
	})
	return canonical
}

func usedHostPortsByNode(tasks []Task) map[string][]HostPort {
	used := make(map[string][]HostPort)
	for _, task := range tasks {
		if task.NodeID == "" || terminalTaskState(task.State) {
			continue
		}
		for _, port := range task.HostPorts {
			if !slices.Contains(used[task.NodeID], port) {
				used[task.NodeID] = append(used[task.NodeID], port)
			}
		}
	}
	for nodeID := range used {
		sort.Slice(used[nodeID], func(a, b int) bool {
			if used[nodeID][a].PublishedPort != used[nodeID][b].PublishedPort {
				return used[nodeID][a].PublishedPort < used[nodeID][b].PublishedPort
			}
			return used[nodeID][a].Protocol < used[nodeID][b].Protocol
		})
	}
	return used
}

func activeTasksByNode(tasks []Task, serviceID string) map[string]uint64 {
	active := make(map[string]uint64)
	for _, task := range tasks {
		if task.ServiceID != serviceID || task.NodeID == "" || terminalTaskState(task.State) {
			continue
		}
		active[task.NodeID]++
	}
	return active
}

func reservedResourcesByNode(tasks []Task) map[string]Resources {
	reserved := make(map[string]Resources)
	for _, task := range tasks {
		if task.NodeID == "" || terminalTaskState(task.State) {
			continue
		}
		resources := reserved[task.NodeID]
		resources.NanoCPUs += task.Reservations.NanoCPUs
		resources.MemoryBytes += task.Reservations.MemoryBytes
		reserved[task.NodeID] = resources
	}
	return reserved
}

func availableResources(capacity, reserved Resources) Resources {
	return Resources{
		NanoCPUs:    max(capacity.NanoCPUs-reserved.NanoCPUs, 0),
		MemoryBytes: max(capacity.MemoryBytes-reserved.MemoryBytes, 0),
	}
}

func formatCPUs(nanoCPUs int64) string {
	unit := "CPUs"
	if nanoCPUs == 1_000_000_000 {
		unit = "CPU"
	}
	return fmt.Sprintf("%.3g %s", float64(nanoCPUs)/1_000_000_000, unit)
}

func formatMemory(bytes int64) string {
	const (
		gibibyte = int64(1 << 30)
		mebibyte = int64(1 << 20)
		kibibyte = int64(1 << 10)
	)
	switch {
	case bytes >= gibibyte:
		return fmt.Sprintf("%.3g GiB", float64(bytes)/float64(gibibyte))
	case bytes >= mebibyte:
		return fmt.Sprintf("%.3g MiB", float64(bytes)/float64(mebibyte))
	case bytes >= kibibyte:
		return fmt.Sprintf("%.3g KiB", float64(bytes)/float64(kibibyte))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func matchesRequiredPlatform(node Platform, required []Platform) bool {
	for _, platform := range required {
		osMatches := platform.OS == "" || strings.EqualFold(platform.OS, node.OS)
		architectureMatches := platform.Architecture == "" || normalizeArchitecture(platform.Architecture) == normalizeArchitecture(node.Architecture)
		if osMatches && architectureMatches {
			return true
		}
	}
	return false
}

func normalizeArchitecture(architecture string) string {
	switch strings.ToLower(architecture) {
	case "x86_64":
		return "amd64"
	case "aarch64":
		return "arm64"
	default:
		return strings.ToLower(architecture)
	}
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
	supported := key == "node.id" || key == "node.hostname" || key == "node.role" || key == "node.platform.os" || key == "node.platform.arch" || strings.HasPrefix(key, "node.labels.") && len(key) > len("node.labels.")
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
	case c.key == "node.platform.os":
		actual = node.Platform.OS
	case c.key == "node.platform.arch":
		actual = node.Platform.Architecture
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
