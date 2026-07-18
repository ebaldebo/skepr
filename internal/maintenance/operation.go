package maintenance

import (
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const OperationSchemaVersion = 1

type Operation struct {
	SchemaVersion    int                      `json:"schema_version"`
	ID               string                   `json:"id"`
	ClusterID        string                   `json:"cluster_id"`
	Endpoint         string                   `json:"endpoint"`
	Target           status.Node              `json:"target"`
	Managers         []status.Node            `json:"managers"`
	TargetWorkload   preflight.TargetWorkload `json:"target_workload"`
	Phase            Phase                    `json:"phase"`
	PhaseTimestamps  map[Phase]time.Time      `json:"phase_timestamps"`
	MutationOccurred bool                     `json:"mutation_occurred"`
	LastError        string                   `json:"last_error,omitempty"`
	CreatedAt        time.Time                `json:"created_at"`
	UpdatedAt        time.Time                `json:"updated_at"`
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
