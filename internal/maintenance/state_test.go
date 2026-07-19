package maintenance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current Phase
		next    Phase
		wantErr string
	}{
		{name: "created to preflight passed", current: PhaseCreated, next: PhasePreflightPassed},
		{name: "preflight passed to draining", current: PhasePreflightPassed, next: PhaseDraining},
		{name: "draining to evacuating", current: PhaseDraining, next: PhaseEvacuating},
		{name: "evacuating to waiting services", current: PhaseEvacuating, next: PhaseWaitingServices},
		{name: "waiting services to maintenance ready", current: PhaseWaitingServices, next: PhaseMaintenanceReady},
		{name: "waiting services to reconciling", current: PhaseWaitingServices, next: PhaseReconciling},
		{name: "reconciling to waiting services", current: PhaseReconciling, next: PhaseWaitingServices},
		{name: "maintenance ready to verifying return", current: PhaseMaintenanceReady, next: PhaseVerifyingReturn},
		{name: "verifying return to activating", current: PhaseVerifyingReturn, next: PhaseActivating},
		{name: "activating to verifying cluster", current: PhaseActivating, next: PhaseVerifyingCluster},
		{name: "verifying cluster to completed", current: PhaseVerifyingCluster, next: PhaseCompleted},
		{name: "cannot skip preflight", current: PhaseCreated, next: PhaseDraining, wantErr: "invalid maintenance phase transition from created to draining"},
		{name: "cannot skip evacuation", current: PhaseDraining, next: PhaseWaitingServices, wantErr: "invalid maintenance phase transition from draining to waiting-services"},
		{name: "cannot activate before return verification", current: PhaseMaintenanceReady, next: PhaseActivating, wantErr: "invalid maintenance phase transition from maintenance-ready to activating"},
		{name: "completed is terminal", current: PhaseCompleted, next: PhaseCreated, wantErr: "invalid maintenance phase transition from completed to created"},
		{name: "unknown current phase", current: Phase("unknown"), next: PhaseCreated, wantErr: "invalid maintenance phase transition from unknown to created"},
		{name: "unknown next phase", current: PhaseCreated, next: Phase("unknown"), wantErr: "invalid maintenance phase transition from created to unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateTransition(tt.current, tt.next)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}
