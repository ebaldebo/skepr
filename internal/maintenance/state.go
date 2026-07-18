package maintenance

import "fmt"

type Phase string

const (
	PhaseCreated          Phase = "created"
	PhasePreflightPassed  Phase = "preflight-passed"
	PhaseDraining         Phase = "draining"
	PhaseEvacuating       Phase = "evacuating"
	PhaseWaitingServices  Phase = "waiting-services"
	PhaseMaintenanceReady Phase = "maintenance-ready"
	PhaseReconciling      Phase = "reconciling"
	PhaseVerifyingReturn  Phase = "verifying-return"
	PhaseActivating       Phase = "activating"
	PhaseVerifyingCluster Phase = "verifying-cluster"
	PhaseCompleted        Phase = "completed"
	PhaseAborted          Phase = "aborted"
)

var allowedTransitions = map[Phase]map[Phase]struct{}{
	PhaseCreated: {
		PhasePreflightPassed: {},
		PhaseAborted:         {},
	},
	PhasePreflightPassed: {
		PhaseDraining: {},
		PhaseAborted:  {},
	},
	PhaseDraining: {
		PhaseEvacuating: {},
		PhaseAborted:    {},
	},
	PhaseEvacuating: {
		PhaseWaitingServices: {},
	},
	PhaseWaitingServices: {
		PhaseMaintenanceReady: {},
		PhaseReconciling:      {},
	},
	PhaseReconciling: {
		PhaseWaitingServices: {},
	},
	PhaseMaintenanceReady: {
		PhaseVerifyingReturn: {},
	},
	PhaseVerifyingReturn: {
		PhaseActivating: {},
	},
	PhaseActivating: {
		PhaseVerifyingCluster: {},
	},
	PhaseVerifyingCluster: {
		PhaseCompleted: {},
	},
	PhaseCompleted: {},
	PhaseAborted:   {},
}

func ValidateTransition(current, next Phase) error {
	allowed, exists := allowedTransitions[current]
	if !exists {
		return fmt.Errorf("invalid maintenance phase transition from %s to %s", current, next)
	}
	if _, exists := allowedTransitions[next]; !exists {
		return fmt.Errorf("invalid maintenance phase transition from %s to %s", current, next)
	}
	if _, exists := allowed[next]; !exists {
		return fmt.Errorf("invalid maintenance phase transition from %s to %s", current, next)
	}
	return nil
}
