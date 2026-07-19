package maintenance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const (
	defaultBeginTimeout = 5 * time.Minute
	defaultPollInterval = time.Second
)

type BeginClient interface {
	Inspect(context.Context) (status.Result, error)
	UpdateNodeAvailability(context.Context, string, string) error
}

type Beginner struct {
	Client         BeginClient
	Store          OperationStore
	Now            func() time.Time
	NewOperationID func() (string, error)
	Progress       func(Operation)
	Timeout        time.Duration
	PollInterval   time.Duration
}

type SafetyError struct {
	Result preflight.Result
}

func (e *SafetyError) Error() string {
	return fmt.Sprintf("maintenance preflight blocked for target %s", e.Result.RequestedNode)
}

type BeginError struct {
	OperationID      string
	Phase            Phase
	MutationOccurred bool
	Err              error
}

func (e *BeginError) Error() string {
	return fmt.Sprintf("maintenance begin operation %s failed in phase %s: %v", e.OperationID, e.Phase, e.Err)
}

func (e *BeginError) Unwrap() error {
	return e.Err
}

func (b Beginner) Begin(ctx context.Context, requestedNode string) (Operation, error) {
	initialInventory, err := b.Client.Inspect(ctx)
	if err != nil {
		return Operation{}, fmt.Errorf("inspect Swarm before maintenance: %w", err)
	}
	initialCheck := preflight.CheckNode(initialInventory, requestedNode)
	if !initialCheck.Safe {
		return Operation{}, &SafetyError{Result: initialCheck}
	}

	lock, err := b.Store.AcquireClusterLock(initialInventory.Cluster.ID)
	if err != nil {
		return Operation{}, err
	}
	defer func() { _ = lock.Release() }()
	if err := b.Store.EnsureNoActiveOperation(initialInventory.Cluster.ID); err != nil {
		return Operation{}, err
	}

	inventory, err := b.Client.Inspect(ctx)
	if err != nil {
		return Operation{}, fmt.Errorf("repeat Swarm inspection before maintenance: %w", err)
	}
	check := preflight.CheckNode(inventory, requestedNode)
	if !check.Safe {
		return Operation{}, &SafetyError{Result: check}
	}
	if inventory.Cluster.ID != initialInventory.Cluster.ID {
		return Operation{}, fmt.Errorf("connected Swarm changed from %s to %s before maintenance", initialInventory.Cluster.ID, inventory.Cluster.ID)
	}
	if check.Target == nil || initialCheck.Target == nil || check.Target.ID != initialCheck.Target.ID {
		return Operation{}, fmt.Errorf("target node %s changed before maintenance", requestedNode)
	}

	now := b.now()
	operationID, err := b.newOperationID()
	if err != nil {
		return Operation{}, fmt.Errorf("generate maintenance operation ID: %w", err)
	}
	operation := Operation{
		SchemaVersion:   OperationSchemaVersion,
		ID:              operationID,
		ClusterID:       inventory.Cluster.ID,
		Endpoint:        inventory.Endpoint,
		Target:          *check.Target,
		Managers:        managerSnapshot(inventory.Nodes),
		TargetWorkload:  *check.TargetWorkload,
		Phase:           PhaseCreated,
		PhaseTimestamps: map[Phase]time.Time{PhaseCreated: now},
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := b.Store.Save(operation); err != nil {
		return Operation{}, fmt.Errorf("persist maintenance operation %s: %w", operation.ID, err)
	}
	b.reportProgress(operation)
	if err := b.transitionAndSave(&operation, PhasePreflightPassed); err != nil {
		return operation, err
	}
	if err := b.transitionAndSave(&operation, PhaseDraining); err != nil {
		return operation, err
	}
	mutationCtx, cancel := context.WithTimeout(ctx, b.timeout())
	defer cancel()
	operation.MutationOccurred = true
	operation.UpdatedAt = b.now()
	if err := b.Store.Save(operation); err != nil {
		return operation, fmt.Errorf("persist mutation intent for operation %s: %w", operation.ID, err)
	}
	if err := b.Client.UpdateNodeAvailability(mutationCtx, operation.Target.ID, "drain"); err != nil {
		return b.fail(operation, fmt.Errorf("drain target node %s: %w", operation.Target.Hostname, err))
	}
	if err := b.transitionAndSave(&operation, PhaseEvacuating); err != nil {
		return b.fail(operation, err)
	}

	if err := b.waitForEvacuation(mutationCtx, operation.Target.ID); err != nil {
		return b.fail(operation, fmt.Errorf("wait for target evacuation: %w", err))
	}
	if err := b.transitionAndSave(&operation, PhaseWaitingServices); err != nil {
		return b.fail(operation, err)
	}
	if err := b.waitForAffectedServices(mutationCtx, operation.TargetWorkload.AffectedServices); err != nil {
		return b.fail(operation, fmt.Errorf("wait for affected services: %w", err))
	}
	if err := b.transitionAndSave(&operation, PhaseMaintenanceReady); err != nil {
		return b.fail(operation, err)
	}
	return operation, nil
}

func (b Beginner) transitionAndSave(operation *Operation, next Phase) error {
	if err := operation.transition(next, b.now()); err != nil {
		return err
	}
	if err := b.Store.Save(*operation); err != nil {
		return fmt.Errorf("persist maintenance operation %s in phase %s: %w", operation.ID, operation.Phase, err)
	}
	b.reportProgress(*operation)
	return nil
}

func (b Beginner) fail(operation Operation, cause error) (Operation, error) {
	operation.LastError = cause.Error()
	operation.UpdatedAt = b.now()
	if err := b.Store.Save(operation); err != nil {
		cause = fmt.Errorf("%v; persist operation error: %w", cause, err)
	}
	return operation, &BeginError{
		OperationID:      operation.ID,
		Phase:            operation.Phase,
		MutationOccurred: operation.MutationOccurred,
		Err:              cause,
	}
}

func (b Beginner) waitForEvacuation(ctx context.Context, targetID string) error {
	for {
		inventory, err := b.Client.Inspect(ctx)
		if err != nil {
			return err
		}
		evacuated := true
		for _, task := range inventory.DesiredTasks {
			if task.DesiredState == "running" && task.NodeID == targetID {
				evacuated = false
				break
			}
		}
		if evacuated {
			return nil
		}
		if err := b.wait(ctx); err != nil {
			return err
		}
	}
}

func (b Beginner) waitForAffectedServices(ctx context.Context, affected []preflight.AffectedService) error {
	if len(affected) == 0 {
		return nil
	}
	affectedIDs := make(map[string]struct{}, len(affected))
	for _, service := range affected {
		affectedIDs[service.ID] = struct{}{}
	}
	for {
		inventory, err := b.Client.Inspect(ctx)
		if err != nil {
			return err
		}
		converged := 0
		for _, service := range inventory.Services {
			if _, exists := affectedIDs[service.ID]; exists && service.Converged {
				converged++
			}
		}
		if converged == len(affectedIDs) {
			return nil
		}
		if err := b.wait(ctx); err != nil {
			return err
		}
	}
}

func (b Beginner) wait(ctx context.Context) error {
	timer := time.NewTimer(b.pollInterval())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b Beginner) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return time.Now().UTC()
}

func (b Beginner) newOperationID() (string, error) {
	if b.NewOperationID != nil {
		return b.NewOperationID()
	}
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func (b Beginner) timeout() time.Duration {
	if b.Timeout > 0 {
		return b.Timeout
	}
	return defaultBeginTimeout
}

func (b Beginner) pollInterval() time.Duration {
	if b.PollInterval > 0 {
		return b.PollInterval
	}
	return defaultPollInterval
}

func (b Beginner) reportProgress(operation Operation) {
	if b.Progress != nil {
		b.Progress(operation)
	}
}

func managerSnapshot(nodes []status.Node) []status.Node {
	managers := make([]status.Node, 0)
	for _, node := range nodes {
		if node.Role == "manager" {
			managers = append(managers, node)
		}
	}
	return managers
}
