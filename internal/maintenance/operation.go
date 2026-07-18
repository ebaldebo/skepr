package maintenance

import (
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const OperationSchemaVersion = 1

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
	Run                    *RunState                `json:"run,omitempty"`
	LastError              string                   `json:"last_error,omitempty"`
	CreatedAt              time.Time                `json:"created_at"`
	UpdatedAt              time.Time                `json:"updated_at"`
}

type RunPhase string

const (
	RunPhasePreRunning      RunPhase = "pre-running"
	RunPhasePreFailed       RunPhase = "pre-failed"
	RunPhasePreCompleted    RunPhase = "pre-completed"
	RunPhaseUpdateRunning   RunPhase = "update-running"
	RunPhaseUpdateFailed    RunPhase = "update-failed"
	RunPhaseWaitingUpdate   RunPhase = "waiting-update"
	RunPhaseUpdateCompleted RunPhase = "update-completed"
	RunPhaseWaitingReturn   RunPhase = "waiting-return"
	RunPhaseVerifyRunning   RunPhase = "verify-running"
	RunPhaseVerifyFailed    RunPhase = "verify-failed"
	RunPhaseWaitingVerify   RunPhase = "waiting-verify"
	RunPhaseVerifyCompleted RunPhase = "verify-completed"
	RunPhaseCompleted       RunPhase = "completed"
	RunPhaseAborted         RunPhase = "aborted"
)

type RunState struct {
	Phase           RunPhase               `json:"phase"`
	TargetHostname  string                 `json:"target_hostname"`
	DockerContexts  []string               `json:"docker_contexts"`
	DockerEndpoints []string               `json:"docker_endpoints,omitempty"`
	Commands        RunCommands            `json:"commands"`
	CommandAttempts []CommandAttempt       `json:"command_attempts,omitempty"`
	PhaseTimestamps map[RunPhase]time.Time `json:"phase_timestamps"`
}

func (o Operation) Terminal() bool {
	return o.Phase == PhaseCompleted || o.Phase == PhaseAborted
}

type RunCommands struct {
	Pre    []string `json:"pre,omitempty"`
	Update []string `json:"update"`
	Verify []string `json:"verify,omitempty"`
}

type CommandAttempt struct {
	Hook        string     `json:"hook"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	ExitCode    *int       `json:"exit_code,omitempty"`
	Error       string     `json:"error,omitempty"`
}

type ReconciliationResult string

const (
	ReconciliationStarted   ReconciliationResult = "started"
	ReconciliationConverged ReconciliationResult = "converged"
	ReconciliationFailed    ReconciliationResult = "failed"
)

type ReconciliationAttempt struct {
	ServiceID   string               `json:"service_id"`
	Service     string               `json:"service"`
	StartedAt   time.Time            `json:"started_at"`
	CompletedAt *time.Time           `json:"completed_at,omitempty"`
	Result      ReconciliationResult `json:"result"`
	Error       string               `json:"error,omitempty"`
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
