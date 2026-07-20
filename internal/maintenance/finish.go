package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/ebaldebo/skepr/internal/status"
)

type FinishClient interface {
	Inspect(context.Context) (status.Result, error)
	UpdateNodeAvailability(context.Context, string, string) error
}

type FinishStore interface {
	Load(string) (Operation, error)
	Save(Operation) error
	AcquireClusterLock(string) (ClusterLock, error)
}

type Finisher struct {
	Client       FinishClient
	Store        FinishStore
	Now          func() time.Time
	Progress     func(Operation)
	Timeout      time.Duration
	PollInterval time.Duration
}

type FinishSafetyError struct {
	Err error
}

func (e *FinishSafetyError) Error() string {
	return e.Err.Error()
}

func (e *FinishSafetyError) Unwrap() error {
	return e.Err
}

type FinishError struct {
	OperationID string
	Phase       Phase
	Err         error
}

func (e *FinishError) Error() string {
	return fmt.Sprintf("maintenance finish operation %s failed in phase %s: %v", e.OperationID, e.Phase, e.Err)
}

func (e *FinishError) Unwrap() error {
	return e.Err
}

func (f Finisher) Finish(ctx context.Context, operationID string) (Operation, error) {
	operation, err := f.Store.Load(operationID)
	if err != nil {
		return Operation{}, fmt.Errorf("load maintenance operation %s: %w", operationID, err)
	}
	lock, err := f.Store.AcquireClusterLock(operation.ClusterID)
	if err != nil {
		return Operation{}, err
	}
	defer func() { _ = lock.Release() }()
	operation, err = f.Store.Load(operationID)
	if err != nil {
		return Operation{}, fmt.Errorf("reload maintenance operation %s: %w", operationID, err)
	}
	switch operation.Phase {
	case PhaseMaintenanceReady, PhaseVerifyingReturn, PhaseActivating, PhaseVerifyingCluster:
	default:
		return operation, &FinishSafetyError{Err: fmt.Errorf("operation %s is in phase %s and cannot finish", operation.ID, operation.Phase)}
	}

	finishCtx, cancel := context.WithTimeout(ctx, f.timeout())
	defer cancel()
	var activationInventory *status.Result
	if operation.Phase == PhaseMaintenanceReady || operation.Phase == PhaseVerifyingReturn {
		inventory, inspectErr := f.Client.Inspect(finishCtx)
		if inspectErr != nil {
			return operation, fmt.Errorf("inspect Swarm before finishing maintenance: %w", inspectErr)
		}
		if err := validateFinishState(operation, inventory, "drain"); err != nil {
			return operation, &FinishSafetyError{Err: err}
		}
		activationInventory = &inventory
	}
	if operation.Phase == PhaseMaintenanceReady {
		if err := f.transitionAndSave(&operation, PhaseVerifyingReturn); err != nil {
			return operation, err
		}
	}
	if operation.Phase == PhaseVerifyingReturn {
		if err := f.transitionAndSave(&operation, PhaseActivating); err != nil {
			return operation, err
		}
	}
	if operation.Phase == PhaseActivating {
		var inventory status.Result
		if activationInventory != nil {
			inventory = *activationInventory
		} else {
			var inspectErr error
			inventory, inspectErr = f.Client.Inspect(finishCtx)
			if inspectErr != nil {
				return f.fail(operation, fmt.Errorf("inspect Swarm before resuming activation: %w", inspectErr))
			}
		}
		if activeErr := validateFinishState(operation, inventory, "active"); activeErr != nil {
			if drainErr := validateFinishState(operation, inventory, "drain"); drainErr != nil {
				return operation, &FinishSafetyError{Err: activeErr}
			}
			if err := f.Client.UpdateNodeAvailability(finishCtx, operation.Target.ID, "active"); err != nil {
				return f.fail(operation, fmt.Errorf("activate target node %s: %w", operation.Target.Hostname, err))
			}
		}
		if err := f.transitionAndSave(&operation, PhaseVerifyingCluster); err != nil {
			return f.fail(operation, err)
		}
	}
	if err := f.waitForHealthyCluster(finishCtx, operation); err != nil {
		return f.fail(operation, fmt.Errorf("verify cluster after activating target: %w", err))
	}
	if err := f.transitionAndSave(&operation, PhaseCompleted); err != nil {
		return f.fail(operation, err)
	}
	return operation, nil
}

func validateFinishState(operation Operation, inventory status.Result, targetAvailability string) error {
	if inventory.Cluster.ID != operation.ClusterID {
		return fmt.Errorf("connected Swarm %s does not match operation cluster %s", inventory.Cluster.ID, operation.ClusterID)
	}
	if inventory.Cluster.LocalState != "active" || !inventory.Cluster.ControlAvailable {
		return fmt.Errorf("connected Docker endpoint does not have active Swarm control")
	}
	targetFound := false
	for _, node := range inventory.Nodes {
		if node.ID != operation.Target.ID {
			continue
		}
		targetFound = true
		if node.State != "ready" {
			return fmt.Errorf("target node %s state is %s, expected ready", node.Hostname, node.State)
		}
		if node.Availability != targetAvailability {
			return fmt.Errorf("target node %s availability is %s, expected %s", node.Hostname, node.Availability, targetAvailability)
		}
		break
	}
	if !targetFound {
		return fmt.Errorf("target node %s is missing from the connected Swarm", operation.Target.Hostname)
	}
	if err := validateFinishManagers(inventory.Nodes, operation.Target.ID, targetAvailability); err != nil {
		return err
	}
	for _, service := range inventory.Services {
		if !service.Converged {
			return fmt.Errorf("service %s has %d/%d running tasks", service.Name, service.RunningTasks, service.DesiredTasks)
		}
	}
	return nil
}

func validateFinishManagers(nodes []status.Node, targetID, targetAvailability string) error {
	totalManagers := 0
	healthyManagers := 0
	var firstIssue error
	for _, node := range nodes {
		if node.Role != "manager" {
			continue
		}
		totalManagers++
		expectedAvailability := "active"
		if node.ID == targetID {
			expectedAvailability = targetAvailability
		}
		var issue error
		switch {
		case node.State != "ready":
			issue = fmt.Errorf("swarm manager %s state is %s, expected ready", node.Hostname, node.State)
		case node.Availability != expectedAvailability:
			issue = fmt.Errorf("swarm manager %s availability is %s, expected %s", node.Hostname, node.Availability, expectedAvailability)
		case node.ManagerStatus != "leader" && node.ManagerStatus != "reachable":
			issue = fmt.Errorf("swarm manager %s status is %s, expected leader or reachable", node.Hostname, node.ManagerStatus)
		}
		if issue == nil {
			healthyManagers++
		} else if firstIssue == nil {
			firstIssue = issue
		}
	}
	if totalManagers == 0 {
		return fmt.Errorf("connected Swarm has no manager membership")
	}
	required := totalManagers/2 + 1
	if healthyManagers < required {
		return fmt.Errorf("%d healthy managers do not meet quorum requirement %d of %d", healthyManagers, required, totalManagers)
	}
	if firstIssue != nil {
		return firstIssue
	}
	return nil
}

func (f Finisher) waitForHealthyCluster(ctx context.Context, operation Operation) error {
	var lastError error
	for {
		inventory, err := f.Client.Inspect(ctx)
		if err == nil {
			err = validateFinishState(operation, inventory, "active")
		}
		if err == nil {
			return nil
		}
		lastError = err
		timer := time.NewTimer(f.pollInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%v: %w", lastError, ctx.Err())
		case <-timer.C:
		}
	}
}

func (f Finisher) transitionAndSave(operation *Operation, next Phase) error {
	if err := operation.transition(next, f.now()); err != nil {
		return err
	}
	if err := f.Store.Save(*operation); err != nil {
		return fmt.Errorf("persist maintenance operation %s in phase %s: %w", operation.ID, operation.Phase, err)
	}
	if f.Progress != nil {
		f.Progress(*operation)
	}
	return nil
}

func (f Finisher) fail(operation Operation, cause error) (Operation, error) {
	operation.LastError = cause.Error()
	operation.UpdatedAt = f.now()
	if err := f.Store.Save(operation); err != nil {
		cause = fmt.Errorf("%v; persist operation error: %w", cause, err)
	}
	return operation, &FinishError{OperationID: operation.ID, Phase: operation.Phase, Err: cause}
}

func (f Finisher) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now().UTC()
}

func (f Finisher) timeout() time.Duration {
	if f.Timeout > 0 {
		return f.Timeout
	}
	return defaultMaintenanceTimeout
}

func (f Finisher) pollInterval() time.Duration {
	if f.PollInterval > 0 {
		return f.PollInterval
	}
	return defaultPollInterval
}
