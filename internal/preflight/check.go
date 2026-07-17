package preflight

import (
	"fmt"
	"strings"

	"github.com/ebaldebo/skepr/internal/status"
)

type Level string

const (
	LevelPass    Level = "pass"
	LevelBlocker Level = "blocker"
)

type Finding struct {
	Gate    string `json:"gate"`
	Level   Level  `json:"level"`
	Message string `json:"message"`
}

type Result struct {
	SchemaVersion int          `json:"schema_version"`
	Endpoint      string       `json:"endpoint"`
	RequestedNode string       `json:"requested_node"`
	Target        *status.Node `json:"target,omitempty"`
	Safe          bool         `json:"safe"`
	Findings      []Finding    `json:"findings"`
}

func CheckNode(inventory status.Result, requestedNode string) Result {
	result := Result{
		SchemaVersion: status.SchemaVersion,
		Endpoint:      inventory.Endpoint,
		RequestedNode: requestedNode,
		Safe:          true,
	}

	for _, node := range inventory.Nodes {
		if node.ID != requestedNode && node.Hostname != requestedNode {
			continue
		}

		result.Target = &node
		result.Findings = append(result.Findings, Finding{
			Gate:    "target_exists",
			Level:   LevelPass,
			Message: fmt.Sprintf("target node %s exists with role %s", node.Hostname, node.Role),
		})
		result.addStateFinding(node)
		result.addAvailabilityFinding(node)
		result.addEndpointFindings(inventory.Cluster)
		result.addManagerFindings(inventory.Nodes)
		return result
	}

	result.Safe = false
	result.Findings = append(result.Findings, Finding{
		Gate:    "target_exists",
		Level:   LevelBlocker,
		Message: fmt.Sprintf("target node %s was not found", requestedNode),
	})
	result.addEndpointFindings(inventory.Cluster)
	result.addManagerFindings(inventory.Nodes)
	return result
}

func (r *Result) addStateFinding(node status.Node) {
	finding := Finding{Gate: "target_ready"}
	if node.State == "ready" {
		finding.Level = LevelPass
		finding.Message = fmt.Sprintf("target node %s is ready", node.Hostname)
	} else {
		r.Safe = false
		finding.Level = LevelBlocker
		finding.Message = fmt.Sprintf("target node %s state is %s, expected ready", node.Hostname, node.State)
	}
	r.Findings = append(r.Findings, finding)
}

func (r *Result) addAvailabilityFinding(node status.Node) {
	finding := Finding{Gate: "target_active"}
	if node.Availability == "active" {
		finding.Level = LevelPass
		finding.Message = fmt.Sprintf("target node %s is active", node.Hostname)
	} else {
		r.Safe = false
		finding.Level = LevelBlocker
		finding.Message = fmt.Sprintf("target node %s availability is %s, expected active", node.Hostname, node.Availability)
	}
	r.Findings = append(r.Findings, finding)
}

func (r *Result) addEndpointFindings(cluster status.Cluster) {
	stateFinding := Finding{Gate: "swarm_active"}
	if cluster.LocalState == "active" {
		stateFinding.Level = LevelPass
		stateFinding.Message = "connected Docker endpoint is part of an active Swarm"
	} else {
		r.Safe = false
		stateFinding.Level = LevelBlocker
		stateFinding.Message = fmt.Sprintf("connected Docker endpoint Swarm state is %s, expected active", cluster.LocalState)
	}
	r.Findings = append(r.Findings, stateFinding)

	controlFinding := Finding{Gate: "swarm_control_available"}
	if cluster.ControlAvailable {
		controlFinding.Level = LevelPass
		controlFinding.Message = "connected Docker endpoint provides Swarm manager control"
	} else {
		r.Safe = false
		controlFinding.Level = LevelBlocker
		controlFinding.Message = "connected Docker endpoint does not provide Swarm manager control"
	}
	r.Findings = append(r.Findings, controlFinding)
}

func (r *Result) addManagerFindings(nodes []status.Node) {
	for _, node := range nodes {
		if node.Role != "manager" {
			continue
		}

		var issues []string
		if node.State != "ready" {
			issues = append(issues, fmt.Sprintf("state is %s, expected ready", node.State))
		}
		if node.Availability != "active" {
			issues = append(issues, fmt.Sprintf("availability is %s, expected active", node.Availability))
		}
		if node.ManagerStatus != "leader" && node.ManagerStatus != "reachable" {
			managerStatus := node.ManagerStatus
			if managerStatus == "" {
				managerStatus = "unavailable"
			}
			issues = append(issues, fmt.Sprintf("manager status is %s, expected leader or reachable", managerStatus))
		}

		finding := Finding{Gate: "manager_healthy"}
		if len(issues) == 0 {
			finding.Level = LevelPass
			finding.Message = fmt.Sprintf("Swarm manager %s is healthy (ready, active and %s)", node.Hostname, node.ManagerStatus)
		} else {
			r.Safe = false
			finding.Level = LevelBlocker
			finding.Message = fmt.Sprintf("Swarm manager %s is unhealthy: %s", node.Hostname, strings.Join(issues, "; "))
		}
		r.Findings = append(r.Findings, finding)
	}
}
