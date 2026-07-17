package preflight

import (
	"fmt"

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
			Message: fmt.Sprintf("node %s exists", node.Hostname),
		})
		result.addStateFinding(node)
		result.addAvailabilityFinding(node)
		return result
	}

	result.Safe = false
	result.Findings = append(result.Findings, Finding{
		Gate:    "target_exists",
		Level:   LevelBlocker,
		Message: fmt.Sprintf("node %s was not found", requestedNode),
	})
	return result
}

func (r *Result) addStateFinding(node status.Node) {
	finding := Finding{Gate: "target_ready"}
	if node.State == "ready" {
		finding.Level = LevelPass
		finding.Message = fmt.Sprintf("node %s is ready", node.Hostname)
	} else {
		r.Safe = false
		finding.Level = LevelBlocker
		finding.Message = fmt.Sprintf("node %s state is %s, expected ready", node.Hostname, node.State)
	}
	r.Findings = append(r.Findings, finding)
}

func (r *Result) addAvailabilityFinding(node status.Node) {
	finding := Finding{Gate: "target_active"}
	if node.Availability == "active" {
		finding.Level = LevelPass
		finding.Message = fmt.Sprintf("node %s is active", node.Hostname)
	} else {
		r.Safe = false
		finding.Level = LevelBlocker
		finding.Message = fmt.Sprintf("node %s availability is %s, expected active", node.Hostname, node.Availability)
	}
	r.Findings = append(r.Findings, finding)
}
