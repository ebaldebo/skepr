package maintenance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/ebaldebo/skepr/internal/status"
)

type RunClient interface {
	Inspect(context.Context) (status.Result, error)
	UpdateNodeAvailability(context.Context, string, string) error
	ForceUpdateService(context.Context, string) error
}

type RunStore interface {
	OperationStore
	Load(string) (Operation, error)
}

type CommandExecutor interface {
	Run(context.Context, []string, io.Writer, io.Writer) error
}

type OSCommandExecutor struct{}

func (OSCommandExecutor) Run(ctx context.Context, argv []string, stdout, stderr io.Writer) error {
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Stdout = stdout
	command.Stderr = stderr
	return command.Run()
}

type TransactionRunner struct {
	Client        RunClient
	Store         RunStore
	Executor      CommandExecutor
	CommandOutput io.Writer
	Progress      func(Operation)
	Timeout       time.Duration
	ReturnTimeout time.Duration
	PollInterval  time.Duration
	Now           func() time.Time
}

type TransactionError struct {
	OperationID       string
	Drained           bool
	ActivationStarted bool
	Err               error
}

func (e *TransactionError) Error() string {
	if e.OperationID == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("maintenance run operation %s failed: %v", e.OperationID, e.Err)
}

func (e *TransactionError) Unwrap() error { return e.Err }

func (r TransactionRunner) Run(ctx context.Context, target string, commands RunCommands, dockerContexts []string) (Operation, error) {
	beginner := Beginner{
		Client:       r.Client,
		Store:        r.Store,
		Timeout:      r.timeout(),
		PollInterval: r.pollInterval(),
		Progress:     r.Progress,
		Initialize: func(operation *Operation) {
			operation.Run = &RunState{
				TargetHostname:  target,
				DockerContexts:  append([]string(nil), dockerContexts...),
				Commands:        commands,
				PhaseTimestamps: make(map[RunPhase]time.Time),
			}
		},
		BeforeDrain: func(ctx context.Context, operation *Operation) error {
			now := r.now()
			if len(commands.Pre) == 0 {
				operation.Run.Phase = RunPhasePreCompleted
				operation.Run.PhaseTimestamps[RunPhasePreCompleted] = now
				return r.Store.Save(*operation)
			}
			return r.executeHook(ctx, operation, "pre", commands.Pre, RunPhasePreRunning, RunPhasePreCompleted, RunPhasePreFailed)
		},
	}
	operation, err := beginner.Begin(ctx, target)
	if err != nil {
		var beginError *BeginError
		if !errors.As(err, &beginError) || operation.Phase != PhaseWaitingServices {
			return operation, r.transactionError(operation, err)
		}
	}
	return r.continueRun(ctx, operation)
}

func (r TransactionRunner) Resume(ctx context.Context, operationID string) (Operation, error) {
	operation, err := r.Store.Load(operationID)
	if err != nil {
		return Operation{}, fmt.Errorf("load maintenance run operation %s: %w", operationID, err)
	}
	if operation.Run == nil {
		return operation, &TransactionError{OperationID: operation.ID, Drained: operation.MutationOccurred, Err: fmt.Errorf("operation was not created by maintenance run")}
	}
	return r.continueRun(ctx, operation)
}

func (r TransactionRunner) continueRun(ctx context.Context, operation Operation) (Operation, error) {
	var err error
	if operation.Phase == PhaseCreated || operation.Phase == PhasePreflightPassed || operation.Phase == PhaseDraining || operation.Phase == PhaseEvacuating {
		beforeDrain := func(ctx context.Context, operation *Operation) error {
			if operation.Run.Phase == RunPhasePreCompleted {
				return nil
			}
			if len(operation.Run.Commands.Pre) == 0 {
				return r.setRunPhase(operation, RunPhasePreCompleted)
			}
			return r.executeHook(ctx, operation, "pre", operation.Run.Commands.Pre, RunPhasePreRunning, RunPhasePreCompleted, RunPhasePreFailed)
		}
		operation, err = (Beginner{
			Client: r.Client, Store: r.Store, Timeout: r.timeout(), PollInterval: r.pollInterval(), Progress: r.Progress, BeforeDrain: beforeDrain,
		}).Resume(ctx, operation.ID)
		if err != nil && operation.Phase != PhaseWaitingServices {
			return operation, r.transactionError(operation, err)
		}
	}
	if operation.Phase == PhaseWaitingServices {
		operation, err = (Reconciler{
			Client: r.Client, Store: r.Store, Timeout: r.timeout(), PollInterval: r.pollInterval(), Progress: r.Progress,
		}).Reconcile(ctx, operation.ID)
		if err != nil {
			return operation, r.transactionError(operation, err)
		}
	}
	if operation.Phase == PhaseCompleted {
		if operation.Run != nil && operation.Run.Phase != RunPhaseCompleted {
			if err := r.setRunPhase(&operation, RunPhaseCompleted); err != nil {
				return operation, r.transactionError(operation, err)
			}
		}
		return operation, nil
	}
	if operation.Phase == PhaseVerifyingReturn || operation.Phase == PhaseActivating || operation.Phase == PhaseVerifyingCluster {
		operation, err = (Finisher{
			Client: r.Client, Store: r.Store, Timeout: r.timeout(), PollInterval: r.pollInterval(), Progress: r.Progress,
		}).Finish(ctx, operation.ID)
		if err != nil {
			return operation, r.transactionError(operation, err)
		}
		if err := r.setRunPhase(&operation, RunPhaseCompleted); err != nil {
			return operation, r.transactionError(operation, err)
		}
		return operation, nil
	}
	if operation.Phase != PhaseMaintenanceReady {
		return operation, r.transactionError(operation, fmt.Errorf("operation is in phase %s and cannot yet be resumed", operation.Phase))
	}

	startingRunPhase := operation.Run.Phase
	switch startingRunPhase {
	case RunPhasePreCompleted, RunPhaseUpdateRunning, RunPhaseUpdateFailed, RunPhaseWaitingUpdate:
		if err := r.waitForReturn(ctx, &operation, RunPhaseWaitingUpdate); err != nil {
			return operation, r.transactionError(operation, err)
		}
		if err := r.executeHook(ctx, &operation, "update", operation.Run.Commands.Update, RunPhaseUpdateRunning, RunPhaseUpdateCompleted, RunPhaseUpdateFailed); err != nil {
			return operation, r.transactionError(operation, err)
		}
		fallthrough
	case RunPhaseUpdateCompleted, RunPhaseWaitingReturn:
		if err := r.waitForReturn(ctx, &operation, RunPhaseWaitingReturn); err != nil {
			return operation, r.transactionError(operation, err)
		}
		fallthrough
	case RunPhaseVerifyRunning, RunPhaseVerifyFailed, RunPhaseWaitingVerify:
		if startingRunPhase == RunPhaseVerifyRunning || startingRunPhase == RunPhaseVerifyFailed || startingRunPhase == RunPhaseWaitingVerify {
			if err := r.waitForReturn(ctx, &operation, RunPhaseWaitingVerify); err != nil {
				return operation, r.transactionError(operation, err)
			}
		}
		if len(operation.Run.Commands.Verify) > 0 {
			if err := r.executeHook(ctx, &operation, "verify", operation.Run.Commands.Verify, RunPhaseVerifyRunning, RunPhaseVerifyCompleted, RunPhaseVerifyFailed); err != nil {
				return operation, r.transactionError(operation, err)
			}
		} else if err := r.setRunPhase(&operation, RunPhaseVerifyCompleted); err != nil {
			return operation, r.transactionError(operation, err)
		}
		fallthrough
	case RunPhaseVerifyCompleted:
		operation, err = (Finisher{
			Client: r.Client, Store: r.Store, Timeout: r.timeout(), PollInterval: r.pollInterval(), Progress: r.Progress,
		}).Finish(ctx, operation.ID)
		if err != nil {
			return operation, r.transactionError(operation, err)
		}
		if err := r.setRunPhase(&operation, RunPhaseCompleted); err != nil {
			return operation, r.transactionError(operation, err)
		}
	case RunPhaseCompleted:
		return operation, nil
	default:
		return operation, r.transactionError(operation, fmt.Errorf("maintenance run has unsupported phase %s", operation.Run.Phase))
	}
	return operation, nil
}

func (r TransactionRunner) executeHook(ctx context.Context, operation *Operation, hook string, argv []string, running, completed, failed RunPhase) error {
	if len(argv) == 0 {
		return fmt.Errorf("%s command is empty", hook)
	}
	if err := r.setRunPhase(operation, running); err != nil {
		return err
	}
	attempt := CommandAttempt{Hook: hook, StartedAt: r.now()}
	operation.Run.CommandAttempts = append(operation.Run.CommandAttempts, attempt)
	if err := r.Store.Save(*operation); err != nil {
		return fmt.Errorf("persist %s command intent: %w", hook, err)
	}
	commandCtx, cancel := context.WithTimeout(ctx, r.timeout())
	defer cancel()
	err := r.executor().Run(commandCtx, argv, r.CommandOutput, r.CommandOutput)
	completedAt := r.now()
	attempt = operation.Run.CommandAttempts[len(operation.Run.CommandAttempts)-1]
	attempt.CompletedAt = &completedAt
	exitCode := 0
	if err != nil {
		exitCode = -1
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		}
		attempt.Error = err.Error()
	}
	attempt.ExitCode = &exitCode
	operation.Run.CommandAttempts[len(operation.Run.CommandAttempts)-1] = attempt
	if err != nil {
		operation.Run.Phase = failed
		operation.Run.PhaseTimestamps[failed] = completedAt
		operation.LastError = fmt.Sprintf("%s command failed: %v", hook, err)
		operation.UpdatedAt = completedAt
		if saveErr := r.Store.Save(*operation); saveErr != nil {
			return fmt.Errorf("%s; persist command failure: %w", operation.LastError, saveErr)
		}
		return errors.New(operation.LastError)
	}
	return r.setRunPhase(operation, completed)
}

func (r TransactionRunner) waitForReturn(ctx context.Context, operation *Operation, phase RunPhase) error {
	if err := r.setRunPhase(operation, phase); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, r.returnTimeout())
	defer cancel()
	var lastError error
	for {
		inventory, err := r.Client.Inspect(waitCtx)
		if err == nil {
			err = validateReturnedTarget(*operation, inventory)
		}
		if err == nil {
			return nil
		}
		lastError = err
		timer := time.NewTimer(r.pollInterval())
		select {
		case <-waitCtx.Done():
			timer.Stop()
			operation.LastError = fmt.Sprintf("wait for target return: %v: %v", lastError, waitCtx.Err())
			operation.UpdatedAt = r.now()
			_ = r.Store.Save(*operation)
			return errors.New(operation.LastError)
		case <-timer.C:
		}
	}
}

func validateReturnedTarget(operation Operation, inventory status.Result) error {
	if err := validateFinishState(operation, inventory, "drain"); err != nil {
		return err
	}
	for _, node := range inventory.Nodes {
		if node.ID != operation.Target.ID {
			continue
		}
		if node.Hostname != operation.Target.Hostname {
			return fmt.Errorf("target node ID %s hostname changed from %s to %s", node.ID, operation.Target.Hostname, node.Hostname)
		}
		return nil
	}
	return fmt.Errorf("target node %s is missing from the connected Swarm", operation.Target.Hostname)
}

func (r TransactionRunner) setRunPhase(operation *Operation, phase RunPhase) error {
	now := r.now()
	operation.Run.Phase = phase
	if operation.Run.PhaseTimestamps == nil {
		operation.Run.PhaseTimestamps = make(map[RunPhase]time.Time)
	}
	operation.Run.PhaseTimestamps[phase] = now
	operation.UpdatedAt = now
	operation.LastError = ""
	if err := r.Store.Save(*operation); err != nil {
		return fmt.Errorf("persist maintenance run phase %s: %w", phase, err)
	}
	if r.Progress != nil {
		r.Progress(*operation)
	}
	return nil
}

func (r TransactionRunner) transactionError(operation Operation, err error) error {
	activationStarted := operation.Phase == PhaseActivating || operation.Phase == PhaseVerifyingCluster
	return &TransactionError{
		OperationID:       operation.ID,
		Drained:           operation.MutationOccurred && operation.Phase != PhaseCompleted && !activationStarted,
		ActivationStarted: activationStarted,
		Err:               err,
	}
}

func (r TransactionRunner) executor() CommandExecutor {
	if r.Executor != nil {
		return r.Executor
	}
	return OSCommandExecutor{}
}

func (r TransactionRunner) timeout() time.Duration {
	if r.Timeout > 0 {
		return r.Timeout
	}
	return defaultBeginTimeout
}

func (r TransactionRunner) returnTimeout() time.Duration {
	if r.ReturnTimeout > 0 {
		return r.ReturnTimeout
	}
	return 30 * time.Minute
}

func (r TransactionRunner) pollInterval() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return defaultPollInterval
}

func (r TransactionRunner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now().UTC()
}
