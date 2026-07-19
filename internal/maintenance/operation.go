package maintenance

import (
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const OperationSchemaVersion = 2

type Operation struct {
	SchemaVersion          int                      `json:"schema_version"`
	ID                     string                   `json:"id"`
	ClusterID              string                   `json:"cluster_id"`
	Endpoint               string                   `json:"endpoint"`
	Target                 status.Node              `json:"target"`
	Managers               []status.Node            `json:"managers"`
	TargetWorkload         preflight.TargetWorkload `json:"target_workload"`
	Phase                  Phase                    `json:"phase"`
	PhaseTimestamps        map[Phase]time.Time      `json:"phase_timestamps"`
	MutationOccurred       bool                     `json:"mutation_occurred"`
	ReconciliationAttempts []ReconciliationAttempt  `json:"reconciliation_attempts,omitempty"`
	LastError              string                   `json:"last_error,omitempty"`
	CreatedAt              time.Time                `json:"created_at"`
	UpdatedAt              time.Time                `json:"updated_at"`
}

type ReconciliationResult string

const (
	ReconciliationStarted   ReconciliationResult = "started"
	ReconciliationConverged ReconciliationResult = "converged"
)

type ReconciliationAttempt struct {
	ServiceID         string               `json:"service_id"`
	Service           string               `json:"service"`
	ForceUpdateBefore *uint64              `json:"force_update_before,omitempty"`
	StartedAt         time.Time            `json:"started_at"`
	CompletedAt       *time.Time           `json:"completed_at,omitempty"`
	Result            ReconciliationResult `json:"result"`
	Error             string               `json:"error,omitempty"`
}

func (o *Operation) transition(next Phase, now time.Time) error {
	if err := ValidateTransition(o.Phase, next); err != nil {
		return err
	}
	o.Phase = next
	o.PhaseTimestamps[next] = now
	o.UpdatedAt = now
	o.LastError = ""
	return nil
}

type ClusterLock interface {
	Release() error
}

type OperationStore interface {
	Save(Operation) error
	EnsureNoActiveOperation(string) error
	AcquireClusterLock(string) (ClusterLock, error)
}
