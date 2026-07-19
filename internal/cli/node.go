package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ebaldebo/skepr/internal/activate"
	"github.com/ebaldebo/skepr/internal/drain"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

func runNode(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		report(stderr, "usage: skepr [--context name] node <command>\n")
		return ExitInvalidUsage
	}
	switch args[0] {
	case "drain":
		return runNodeDrain(ctx, args[1:], contextName, connector, stdout, stderr)
	case "activate":
		return runNodeActivate(ctx, args[1:], contextName, connector, stdout, stderr)
	default:
		report(stderr, "usage: skepr [--context name] node <command>\n")
		return ExitInvalidUsage
	}
}

func runNodeDrain(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("node drain", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dryRun := flags.Bool("dry-run", false, "preview drain impact without changing the node")
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	timeout := flags.Duration("timeout", 5*time.Minute, "node drain timeout")
	if err := flags.Parse(normalizeNodeDrainArgs(args)); err != nil || flags.NArg() != 1 || *timeout <= 0 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] node drain <node> [--dry-run] [--timeout duration] [--json]\n")
		}
		return ExitInvalidUsage
	}

	connection, err := connector.Connect(ctx, contextName)
	if err != nil {
		report(stderr, "configure Docker connection: %v\n", err)
		return ExitDockerConnection
	}
	defer func() { _ = connection.Close() }()
	if *dryRun {
		inventory, err := connection.Inspect(ctx)
		if err != nil {
			report(stderr, "inspect Docker Swarm: %v\n", err)
			return ExitDockerConnection
		}
		preview := drain.BuildPreview(inventory, flags.Arg(0))
		if err := writeNodeJSONOrHuman(*jsonOutput, stdout, preview, writeDrainPreview); err != nil {
			report(stderr, "write node drain preview output: %v\n", err)
			return ExitDockerConnection
		}
		if !preview.SafeToDrain {
			return ExitSafetyGate
		}
		return ExitSuccess
	}
	maintenanceConnection, ok := connection.(status.MaintenanceConnection)
	if !ok {
		report(stderr, "Docker connection does not support Swarm node updates\n")
		return ExitDockerConnection
	}
	stateDir, err := operations.DefaultStateDir()
	if err != nil {
		report(stderr, "configure node drain state: %v\n", err)
		return ExitInvalidUsage
	}
	drainer := drain.Drainer{
		Client:  maintenanceConnection,
		Guard:   nodeDrainGuard{store: operations.NewStore(stateDir)},
		Timeout: *timeout,
		Progress: func(result drain.Result) {
			_, _ = fmt.Fprintf(stderr, "node drain %s: %s\n", result.Target.Hostname, result.Phase)
		},
	}
	result, err := drainer.Drain(ctx, flags.Arg(0))
	if err != nil {
		return reportNodeDrainError(err, *jsonOutput, stdout, stderr)
	}
	if err := writeNodeJSONOrHuman(*jsonOutput, stdout, result, writeNodeDrainResult); err != nil {
		report(stderr, "write node drain output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

func runNodeActivate(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("node activate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	timeout := flags.Duration("timeout", 5*time.Minute, "node activation timeout")
	if err := flags.Parse(normalizeNodeActivateArgs(args)); err != nil || flags.NArg() != 1 || *timeout <= 0 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] node activate <node> [--timeout duration] [--json]\n")
		}
		return ExitInvalidUsage
	}

	connection, err := connector.Connect(ctx, contextName)
	if err != nil {
		report(stderr, "configure Docker connection: %v\n", err)
		return ExitDockerConnection
	}
	defer func() { _ = connection.Close() }()
	maintenanceConnection, ok := connection.(status.MaintenanceConnection)
	if !ok {
		report(stderr, "Docker connection does not support Swarm node updates\n")
		return ExitDockerConnection
	}
	stateDir, err := operations.DefaultStateDir()
	if err != nil {
		report(stderr, "configure node activation state: %v\n", err)
		return ExitInvalidUsage
	}
	activator := activate.Activator{
		Client:  maintenanceConnection,
		Guard:   nodeDrainGuard{store: operations.NewStore(stateDir)},
		Timeout: *timeout,
		Progress: func(result activate.Result) {
			_, _ = fmt.Fprintf(stderr, "node activate %s: %s\n", result.Target.Hostname, result.Phase)
		},
	}
	result, err := activator.Activate(ctx, flags.Arg(0))
	if err != nil {
		return reportNodeActivateError(err, *jsonOutput, stdout, stderr)
	}
	if err := writeNodeJSONOrHuman(*jsonOutput, stdout, result, writeNodeActivateResult); err != nil {
		report(stderr, "write node activation output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

type nodeDrainGuard struct {
	store *operations.Store
}

func (g nodeDrainGuard) AcquireClusterLock(clusterID string) (func() error, error) {
	lock, err := g.store.AcquireClusterLock(clusterID)
	if err != nil {
		return nil, err
	}
	return lock.Release, nil
}

func (g nodeDrainGuard) EnsureNoActiveOperation(clusterID string) error {
	return g.store.EnsureNoActiveOperation(clusterID)
}

func normalizeNodeDrainArgs(args []string) []string {
	flagArgs := make([]string, 0, len(args))
	positional := make([]string, 0, 1)
	for index := 0; index < len(args); index++ {
		switch arg := args[index]; arg {
		case "--dry-run", "--json":
			flagArgs = append(flagArgs, arg)
		case "--timeout":
			flagArgs = append(flagArgs, arg)
			if index+1 < len(args) {
				index++
				flagArgs = append(flagArgs, args[index])
			}
		default:
			if strings.HasPrefix(arg, "--timeout=") {
				flagArgs = append(flagArgs, arg)
			} else {
				positional = append(positional, arg)
			}
		}
	}
	return append(flagArgs, positional...)
}

func normalizeNodeActivateArgs(args []string) []string {
	flagArgs := make([]string, 0, len(args))
	positional := make([]string, 0, 1)
	for index := 0; index < len(args); index++ {
		switch arg := args[index]; arg {
		case "--json":
			flagArgs = append(flagArgs, arg)
		case "--timeout":
			flagArgs = append(flagArgs, arg)
			if index+1 < len(args) {
				index++
				flagArgs = append(flagArgs, args[index])
			}
		default:
			if strings.HasPrefix(arg, "--timeout=") {
				flagArgs = append(flagArgs, arg)
			} else {
				positional = append(positional, arg)
			}
		}
	}
	return append(flagArgs, positional...)
}

func reportNodeDrainError(err error, jsonOutput bool, stdout, stderr io.Writer) int {
	var safetyError *drain.SafetyError
	if errors.As(err, &safetyError) {
		if outputErr := writeNodeJSONOrHuman(jsonOutput, stdout, safetyError.Preview, writeDrainPreview); outputErr != nil {
			report(stderr, "write node drain safety output: %v\n", outputErr)
			return ExitDockerConnection
		}
		return ExitSafetyGate
	}
	var validationError *drain.ValidationError
	if errors.As(err, &validationError) {
		report(stderr, "node drain blocked: %v\n", validationError)
		return ExitSafetyGate
	}
	var mutationError *drain.MutationError
	if errors.As(err, &mutationError) {
		report(stderr, "node drain failed: %v\n", mutationError.Err)
		report(stderr, "RECOVERY: node %s may remain drained; inspect live state before activating it\n", mutationError.Result.Target.Hostname)
		return ExitPartialMutation
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		report(stderr, "node drain stopped before mutation: %v\n", err)
		return ExitTimeout
	}
	report(stderr, "node drain failed before mutation: %v\n", err)
	return ExitDockerConnection
}

func reportNodeActivateError(err error, jsonOutput bool, stdout, stderr io.Writer) int {
	var safetyError *activate.SafetyError
	if errors.As(err, &safetyError) {
		if outputErr := writeNodeJSONOrHuman(jsonOutput, stdout, safetyError.Report, writeNodeActivateSafety); outputErr != nil {
			report(stderr, "write node activation safety output: %v\n", outputErr)
			return ExitDockerConnection
		}
		return ExitSafetyGate
	}
	var validationError *activate.ValidationError
	if errors.As(err, &validationError) {
		report(stderr, "node activation blocked: %v\n", validationError)
		return ExitSafetyGate
	}
	var mutationError *activate.MutationError
	if errors.As(err, &mutationError) {
		report(stderr, "node activation failed: %v\n", mutationError.Err)
		report(stderr, "RECOVERY: node %s may be active; inspect live state before making another availability change\n", mutationError.Result.Target.Hostname)
		return ExitPartialMutation
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		report(stderr, "node activation stopped before mutation: %v\n", err)
		return ExitTimeout
	}
	report(stderr, "node activation failed before mutation: %v\n", err)
	return ExitDockerConnection
}

func writeNodeJSONOrHuman[T any](jsonOutput bool, writer io.Writer, result T, writeHuman func(io.Writer, T) error) error {
	if !jsonOutput {
		return writeHuman(writer, result)
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func writeNodeDrainResult(writer io.Writer, result drain.Result) error {
	_, err := fmt.Fprintf(
		writer,
		"DRAINED: %s\nTarget: %s (%s)\nAvailability: %s\nReplicated tasks moved: %d\nGlobal tasks stopped: %d\nAffected services converged: %d\n",
		result.Target.Hostname,
		result.Target.Hostname,
		result.Target.ID,
		result.Availability,
		result.ReplicatedTasksMoved,
		result.GlobalTasksStopped,
		len(result.AffectedServices),
	)
	return err
}

func writeNodeActivateResult(writer io.Writer, result activate.Result) error {
	_, err := fmt.Fprintf(
		writer,
		"ACTIVE: %s\nTarget: %s (%s)\nAvailability: %s\nHealthy managers: %d/%d\nConverged services: %d/%d\n",
		result.Target.Hostname,
		result.Target.Hostname,
		result.Target.ID,
		result.Availability,
		result.HealthyManagers,
		result.TotalManagers,
		result.ConvergedServices,
		result.TotalServices,
	)
	return err
}

func writeNodeActivateSafety(writer io.Writer, report activate.SafetyReport) error {
	targetName := report.RequestedNode
	if report.Target != nil {
		targetName = report.Target.Hostname
	}
	var output strings.Builder
	_, _ = fmt.Fprintf(&output, "ACTIVATION BLOCKED: %s\n", targetName)
	if report.Target == nil {
		output.WriteString("Target: not found\n")
	} else {
		_, _ = fmt.Fprintf(&output, "Target: %s (%s, %s/%s)\n", report.Target.Hostname, report.Target.Role, report.Target.State, report.Target.Availability)
	}
	output.WriteString("\nBlockers:\n")
	if !writeFindingLevel(&output, report.Findings, preflight.LevelBlocker, "  ") {
		output.WriteString("  none\n")
	}
	_, err := io.WriteString(writer, output.String())
	return err
}

func writeDrainPreview(writer io.Writer, preview drain.Preview) error {
	var output strings.Builder
	targetName := preview.RequestedNode
	if preview.Target != nil {
		targetName = preview.Target.Hostname
	}
	drainState := "BLOCKED"
	if preview.SafeToDrain {
		drainState = "SAFE"
	}
	offlineState := "BLOCKED"
	if preview.SafeToTakeOffline {
		offlineState = "SAFE"
	}
	_, _ = fmt.Fprintf(&output, "DRAIN %s: %s\n", drainState, targetName)
	_, _ = fmt.Fprintf(&output, "OFFLINE %s: %s\n", offlineState, targetName)
	if preview.Target == nil {
		output.WriteString("Target: not found\n")
	} else {
		_, _ = fmt.Fprintf(&output, "Target: %s (%s, %s/%s)\n", preview.Target.Hostname, preview.Target.Role, preview.Target.State, preview.Target.Availability)
	}

	output.WriteString("\nDrain blockers:\n")
	if !writeFindingLevel(&output, preview.DrainFindings, preflight.LevelBlocker, "  ") {
		output.WriteString("  none\n")
	}
	writeManagerOfflineFindings(&output, preview)

	output.WriteString("\nReplicated tasks expected to move:\n")
	writePreviewTasks(&output, preview.ReplicatedTasks)
	output.WriteString("Global tasks expected to stop:\n")
	writePreviewTasks(&output, preview.GlobalTasks)
	if len(preview.UnsupportedTasks) > 0 {
		output.WriteString("Unsupported task modes:\n")
		writePreviewTasks(&output, preview.UnsupportedTasks)
	}

	output.WriteString("\nDestination eligibility:\n")
	if len(preview.ServiceImpacts) == 0 {
		output.WriteString("  none\n")
	}
	for _, service := range preview.ServiceImpacts {
		if len(service.EligibleDestinations) == 0 {
			_, _ = fmt.Fprintf(&output, "  %s: no eligible destinations\n", service.Name)
		} else {
			hostnames := make([]string, 0, len(service.EligibleDestinations))
			for _, destination := range service.EligibleDestinations {
				hostnames = append(hostnames, destination.Hostname)
			}
			_, _ = fmt.Fprintf(&output, "  %s: %s\n", service.Name, strings.Join(hostnames, ", "))
		}
		for _, destination := range service.BlockedDestinations {
			reasons := make([]string, 0, len(destination.Blockers))
			for _, blocker := range destination.Blockers {
				reasons = append(reasons, blocker.Message)
			}
			_, _ = fmt.Fprintf(&output, "    %s: %s\n", destination.Hostname, strings.Join(reasons, "; "))
		}
	}

	output.WriteString("\nStorage portability warnings:\n")
	warningCount := 0
	for _, service := range preview.ServiceImpacts {
		for _, warning := range service.StoragePortabilityWarnings {
			_, _ = fmt.Fprintf(&output, "  %s: %s\n", service.Name, warning.Message)
			warningCount++
		}
	}
	if warningCount == 0 {
		output.WriteString("  none\n")
	}
	if len(preview.UnevaluatedInputs) == 0 {
		output.WriteString("Unknown scheduler inputs: none\n")
	} else {
		inputs := make([]string, 0, len(preview.UnevaluatedInputs))
		for _, input := range preview.UnevaluatedInputs {
			inputs = append(inputs, strings.ReplaceAll(input, "_", " "))
		}
		_, _ = fmt.Fprintf(&output, "Unknown scheduler inputs: %s\n", strings.Join(inputs, ", "))
	}
	if len(preview.UnevaluatedConstraints) == 0 {
		output.WriteString("Unevaluated placement constraints: none\n")
	} else {
		constraints := make([]string, 0, len(preview.UnevaluatedConstraints))
		for _, constraint := range preview.UnevaluatedConstraints {
			constraints = append(constraints, constraint.Service+": "+constraint.Constraint)
		}
		_, _ = fmt.Fprintf(&output, "Unevaluated placement constraints: %s\n", strings.Join(constraints, ", "))
	}
	_, err := io.WriteString(writer, output.String())
	return err
}

func writeManagerOfflineFindings(output *strings.Builder, preview drain.Preview) {
	if preview.Target == nil {
		output.WriteString("Manager offline checks: unavailable\n")
		return
	}
	if preview.Target.Role != "manager" {
		output.WriteString("Manager offline checks: not applicable for worker targets\n")
		return
	}
	output.WriteString("Manager offline checks:\n")
	writeFindingLevel(output, preview.ManagerOfflineFindings, preflight.LevelBlocker, "  BLOCKER: ")
	writeFindingLevel(output, preview.ManagerOfflineFindings, preflight.LevelPass, "  PASS: ")
}

func writeFindingLevel(output *strings.Builder, findings []preflight.Finding, level preflight.Level, prefix string) bool {
	wrote := false
	for _, finding := range findings {
		if finding.Level != level {
			continue
		}
		_, _ = fmt.Fprintf(output, "%s%s\n", prefix, finding.Message)
		wrote = true
	}
	return wrote
}

func writePreviewTasks(output *strings.Builder, tasks []preflight.WorkloadTask) {
	if len(tasks) == 0 {
		output.WriteString("  none\n")
		return
	}
	for _, task := range tasks {
		_, _ = fmt.Fprintf(output, "  %s (%s, %s)\n", task.Name, task.Service, task.State)
	}
}
