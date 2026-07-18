package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

type ReconcileClient interface {
	Inspect(context.Context) (status.Result, error)
	ForceUpdateService(context.Context, string) error
}

type ReconcileStore interface {
	Load(string) (Operation, error)
	Save(Operation) error
	AcquireClusterLock(string) (ClusterLock, error)
}

type Reconciler struct {
	Client       ReconcileClient
	Store        ReconcileStore
	Now          func() time.Time
	Progress     func(Operation)
	Timeout      time.Duration
	PollInterval time.Duration
}

type ReconcileSafetyError struct {
	Err error
}

func (e *ReconcileSafetyError) Error() string {
	return e.Err.Error()
}

func (e *ReconcileSafetyError) Unwrap() error {
	return e.Err
}

type ReconcileError struct {
	OperationID string
	Phase       Phase
	Err         error
}

func (e *ReconcileError) Error() string {
	return fmt.Sprintf("maintenance reconcile operation %s failed in phase %s: %v", e.OperationID, e.Phase, e.Err)
}

func (e *ReconcileError) Unwrap() error {
	return e.Err
}

func (r Reconciler) Reconcile(ctx context.Context, operationID string) (Operation, error) {
	operation, err := r.Store.Load(operationID)
	if err != nil {
		return Operation{}, fmt.Errorf("load maintenance operation %s: %w", operationID, err)
	}
	lock, err := r.Store.AcquireClusterLock(operation.ClusterID)
	if err != nil {
		return Operation{}, err
	}
	defer func() { _ = lock.Release() }()
	operation, err = r.Store.Load(operationID)
	if err != nil {
		return Operation{}, fmt.Errorf("reload maintenance operation %s: %w", operationID, err)
	}
	if operation.Phase != PhaseWaitingServices {
		return operation, r.safetyError("operation %s is in phase %s, expected %s", operation.ID, operation.Phase, PhaseWaitingServices)
	}

	inventory, err := r.Client.Inspect(ctx)
	if err != nil {
		return operation, fmt.Errorf("inspect Swarm before reconciliation: %w", err)
	}
	if err := validateReconciliationState(operation, inventory); err != nil {
		return operation, &ReconcileSafetyError{Err: err}
	}
	eligible, err := eligibleReconciliationServices(operation.TargetWorkload.AffectedServices, inventory.Services)
	if err != nil {
		return operation, &ReconcileSafetyError{Err: err}
	}
	if len(eligible) == 0 {
		if err := r.transitionAndSave(&operation, PhaseMaintenanceReady); err != nil {
			return operation, err
		}
		return operation, nil
	}

	reconcileCtx, cancel := context.WithTimeout(ctx, r.timeout())
	defer cancel()
	for _, service := range eligible {
		if err := r.transitionAndSave(&operation, PhaseReconciling); err != nil {
			return operation, err
		}
		operation.ReconciliationAttempts = append(operation.ReconciliationAttempts, ReconciliationAttempt{
			ServiceID: service.ID,
			Service:   service.Name,
			StartedAt: r.now(),
			Result:    ReconciliationStarted,
		})
		operation.UpdatedAt = r.now()
		if err := r.Store.Save(operation); err != nil {
			return operation, fmt.Errorf("persist reconciliation intent for service %s: %w", service.Name, err)
		}
		if err := r.Client.ForceUpdateService(reconcileCtx, service.ID); err != nil {
			return r.fail(operation, fmt.Errorf("force update service %s: %w", service.Name, err))
		}
		if err := r.waitForService(reconcileCtx, operation, service.ID); err != nil {
			return r.fail(operation, fmt.Errorf("wait for service %s: %w", service.Name, err))
		}
		completedAt := r.now()
		attempt := &operation.ReconciliationAttempts[len(operation.ReconciliationAttempts)-1]
		attempt.CompletedAt = &completedAt
		attempt.Result = ReconciliationConverged
		if err := r.transitionAndSave(&operation, PhaseWaitingServices); err != nil {
			return r.fail(operation, err)
		}
	}
	if err := r.transitionAndSave(&operation, PhaseMaintenanceReady); err != nil {
		return r.fail(operation, err)
	}
	return operation, nil
}

func validateReconciliationState(operation Operation, inventory status.Result) error {
	if inventory.Cluster.ID != operation.ClusterID {
		return fmt.Errorf("connected Swarm %s does not match operation cluster %s", inventory.Cluster.ID, operation.ClusterID)
	}
	if inventory.Cluster.LocalState != "active" || !inventory.Cluster.ControlAvailable {
		return fmt.Errorf("connected Docker endpoint does not have active Swarm control")
	}
	for _, node := range inventory.Nodes {
		if node.ID != operation.Target.ID {
			continue
		}
		if node.Availability != "drain" {
			return fmt.Errorf("target node %s availability is %s, expected drain", node.Hostname, node.Availability)
		}
		return nil
	}
	return fmt.Errorf("target node %s is missing from the connected Swarm", operation.Target.Hostname)
}

func eligibleReconciliationServices(saved []preflight.AffectedService, current []status.Service) ([]status.Service, error) {
	currentByID := make(map[string]status.Service, len(current))
	for _, service := range current {
		currentByID[service.ID] = service
	}
	eligible := make([]status.Service, 0)
	for _, affected := range saved {
		service, exists := currentByID[affected.ID]
		if !exists {
			return nil, fmt.Errorf("affected service %s (%s) is missing", affected.Name, affected.ID)
		}
		if service.Converged {
			continue
		}
		if !affected.Singleton || affected.Mode != "replicated" || affected.DesiredTasks != 1 {
			return nil, fmt.Errorf("affected service %s is unconverged and is not a saved replicated singleton", affected.Name)
		}
		if service.Mode != "replicated" || service.RunningTasks != 0 || service.DesiredTasks != 1 {
			return nil, fmt.Errorf("affected service %s is %d/%d in %s mode, expected a replicated singleton at 0/1", service.Name, service.RunningTasks, service.DesiredTasks, service.Mode)
		}
		eligible = append(eligible, service)
	}
	return eligible, nil
}

func (r Reconciler) waitForService(ctx context.Context, operation Operation, serviceID string) error {
	for {
		inventory, err := r.Client.Inspect(ctx)
		if err != nil {
			return err
		}
		if err := validateReconciliationState(operation, inventory); err != nil {
			return err
		}
		for _, service := range inventory.Services {
			if service.ID == serviceID && service.Converged {
				return nil
			}
		}
		timer := time.NewTimer(r.pollInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (r Reconciler) transitionAndSave(operation *Operation, next Phase) error {
	if err := operation.transition(next, r.now()); err != nil {
		return err
	}
	if err := r.Store.Save(*operation); err != nil {
		return fmt.Errorf("persist maintenance operation %s in phase %s: %w", operation.ID, operation.Phase, err)
	}
	if r.Progress != nil {
		r.Progress(*operation)
	}
	return nil
}

func (r Reconciler) fail(operation Operation, cause error) (Operation, error) {
	completedAt := r.now()
	attempt := &operation.ReconciliationAttempts[len(operation.ReconciliationAttempts)-1]
	attempt.CompletedAt = &completedAt
	attempt.Result = ReconciliationFailed
	attempt.Error = cause.Error()
	if operation.Phase == PhaseReconciling {
		if err := operation.transition(PhaseWaitingServices, completedAt); err != nil {
			cause = fmt.Errorf("%v; return operation to waiting-services: %w", cause, err)
		}
	}
	operation.LastError = cause.Error()
	operation.UpdatedAt = completedAt
	if err := r.Store.Save(operation); err != nil {
		cause = fmt.Errorf("%v; persist operation error: %w", cause, err)
	}
	return operation, &ReconcileError{OperationID: operation.ID, Phase: operation.Phase, Err: cause}
}

func (r Reconciler) safetyError(format string, arguments ...any) error {
	return &ReconcileSafetyError{Err: fmt.Errorf(format, arguments...)}
}

func (r Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now().UTC()
}

func (r Reconciler) timeout() time.Duration {
	if r.Timeout > 0 {
		return r.Timeout
	}
	return defaultBeginTimeout
}

func (r Reconciler) pollInterval() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return defaultPollInterval
}
