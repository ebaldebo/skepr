package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/ebaldebo/skepr/internal/drain"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

func runNode(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "drain" {
		report(stderr, "usage: skepr [--context name] node drain <node> --dry-run [--json]\n")
		return ExitInvalidUsage
	}
	return runNodeDrain(ctx, args[1:], contextName, connector, stdout, stderr)
}

func runNodeDrain(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("node drain", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dryRun := flags.Bool("dry-run", false, "preview drain impact without changing the node")
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	if err := flags.Parse(normalizeNodeDrainArgs(args)); err != nil || flags.NArg() != 1 || !*dryRun {
		if err == nil {
			report(stderr, "usage: skepr [--context name] node drain <node> --dry-run [--json]\n")
		}
		return ExitInvalidUsage
	}

	connection, err := connector.Connect(ctx, contextName)
	if err != nil {
		report(stderr, "configure Docker connection: %v\n", err)
		return ExitDockerConnection
	}
	defer func() { _ = connection.Close() }()
	inventory, err := connection.Inspect(ctx)
	if err != nil {
		report(stderr, "inspect Docker Swarm: %v\n", err)
		return ExitDockerConnection
	}
	preview := drain.BuildPreview(inventory, flags.Arg(0))
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(preview); err != nil {
			report(stderr, "write node drain preview output: %v\n", err)
			return ExitDockerConnection
		}
	} else if err := writeDrainPreview(stdout, preview); err != nil {
		report(stderr, "write node drain preview output: %v\n", err)
		return ExitDockerConnection
	}
	if !preview.SafeToDrain {
		return ExitSafetyGate
	}
	return ExitSuccess
}

func normalizeNodeDrainArgs(args []string) []string {
	flagArgs := make([]string, 0, len(args))
	positional := make([]string, 0, 1)
	for _, arg := range args {
		switch arg {
		case "--dry-run", "--json":
			flagArgs = append(flagArgs, arg)
		default:
			positional = append(positional, arg)
		}
	}
	return append(flagArgs, positional...)
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
