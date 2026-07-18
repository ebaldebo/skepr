package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const (
	ExitSuccess          = 0
	ExitSafetyGate       = 1
	ExitInvalidUsage     = 2
	ExitDockerConnection = 3
	ExitTimeout          = 4
	ExitPartialMutation  = 5
)

func Run(ctx context.Context, args []string, connector status.Connector, stdout, stderr io.Writer) int {
	globalFlags := flag.NewFlagSet("skepr", flag.ContinueOnError)
	globalFlags.SetOutput(stderr)
	contextName := globalFlags.String("context", "", "Docker context to use")
	if err := globalFlags.Parse(args); err != nil {
		return ExitInvalidUsage
	}
	args = globalFlags.Args()

	if len(args) == 0 {
		report(stderr, "usage: skepr [--context name] <command>\n")
		return ExitInvalidUsage
	}
	switch args[0] {
	case "status":
		return runStatus(ctx, args[1:], *contextName, connector, stdout, stderr)
	case "check":
		return runCheck(ctx, args[1:], *contextName, connector, stdout, stderr)
	case "maintenance":
		return runMaintenance(ctx, args[1:], *contextName, connector, stdout, stderr)
	default:
		report(stderr, "usage: skepr [--context name] <command>\n")
		return ExitInvalidUsage
	}
}

func runMaintenance(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		report(stderr, "usage: skepr [--context name] maintenance <command>\n")
		return ExitInvalidUsage
	}
	switch args[0] {
	case "begin":
		return runMaintenanceBegin(ctx, args[1:], contextName, connector, stdout, stderr)
	case "reconcile":
		return runMaintenanceReconcile(ctx, args[1:], contextName, connector, stdout, stderr)
	case "show":
		return runMaintenanceShow(ctx, args[1:], contextName, connector, stdout, stderr)
	default:
		report(stderr, "usage: skepr [--context name] maintenance <command>\n")
		return ExitInvalidUsage
	}
}

func runMaintenanceReconcile(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("maintenance reconcile", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	timeout := flags.Duration("timeout", 5*time.Minute, "maintenance reconcile timeout")
	args = normalizeBeginArgs(args)
	if err := flags.Parse(args); err != nil || flags.NArg() != 1 || *timeout <= 0 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] maintenance reconcile <operation-id> [--timeout duration] [--json]\n")
		}
		return ExitInvalidUsage
	}

	connection, err := connector.Connect(ctx, contextName)
	if err != nil {
		report(stderr, "configure Docker connection: %v\n", err)
		return ExitDockerConnection
	}
	defer func() { _ = connection.Close() }()
	reconciliationConnection, ok := connection.(status.ReconciliationConnection)
	if !ok {
		report(stderr, "Docker connection does not support Swarm service reconciliation\n")
		return ExitDockerConnection
	}
	stateDir, err := operations.DefaultStateDir()
	if err != nil {
		report(stderr, "configure operation state: %v\n", err)
		return ExitInvalidUsage
	}
	reconciler := maintenance.Reconciler{
		Client:       reconciliationConnection,
		Store:        operations.NewStore(stateDir),
		Timeout:      *timeout,
		PollInterval: time.Second,
		Progress: func(operation maintenance.Operation) {
			_, _ = fmt.Fprintf(stderr, "operation %s: %s\n", operation.ID, operation.Phase)
		},
	}
	operation, err := reconciler.Reconcile(ctx, flags.Arg(0))
	if err != nil {
		return reportMaintenanceReconcileError(err, stderr)
	}
	result := struct {
		SchemaVersion int                   `json:"schema_version"`
		Operation     maintenance.Operation `json:"operation"`
	}{SchemaVersion: maintenance.OperationSchemaVersion, Operation: operation}
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			report(stderr, "write maintenance reconcile output: %v\n", err)
			return ExitDockerConnection
		}
		return ExitSuccess
	}
	_, err = fmt.Fprintf(stdout, "Operation: %s\nReconciliation attempts: %d\nPhase: %s\n", operation.ID, len(operation.ReconciliationAttempts), operation.Phase)
	if err != nil {
		report(stderr, "write maintenance reconcile output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

func reportMaintenanceReconcileError(err error, stderr io.Writer) int {
	var safety *maintenance.ReconcileSafetyError
	if errors.As(err, &safety) {
		report(stderr, "maintenance reconcile blocked: %v\n", safety)
		return ExitSafetyGate
	}
	var reconcileError *maintenance.ReconcileError
	if errors.As(err, &reconcileError) {
		report(stderr, "%v\n", reconcileError)
		report(stderr, "RECOVERY: node remains drained; inspect operation %s before further mutation\n", reconcileError.OperationID)
		return ExitPartialMutation
	}
	report(stderr, "maintenance reconcile: %v\n", err)
	return ExitDockerConnection
}

func runMaintenanceShow(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("maintenance show", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	if len(args) == 2 && args[1] == "--json" {
		args = []string{"--json", args[0]}
	}
	if err := flags.Parse(args); err != nil || flags.NArg() > 1 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] maintenance show [operation-id] [--json]\n")
		}
		return ExitInvalidUsage
	}
	stateDir, err := operations.DefaultStateDir()
	if err != nil {
		report(stderr, "configure operation state: %v\n", err)
		return ExitInvalidUsage
	}
	store := operations.NewStore(stateDir)
	var operation maintenance.Operation
	if flags.NArg() == 1 {
		operation, err = store.Load(flags.Arg(0))
	} else {
		active, activeErr := store.LatestActive()
		err = activeErr
		if active != nil {
			operation = *active
		}
		if err == nil && active == nil {
			err = fmt.Errorf("no active maintenance operation found")
		}
	}
	if err != nil {
		report(stderr, "load maintenance operation: %v\n", err)
		return ExitInvalidUsage
	}

	result := maintenance.ShowResult{SchemaVersion: maintenance.OperationSchemaVersion, Operation: operation}
	connection, connectionErr := connector.Connect(ctx, contextName)
	if connectionErr != nil {
		result.LiveError = connectionErr.Error()
	} else {
		inventory, inspectErr := connection.Inspect(ctx)
		_ = connection.Close()
		if inspectErr != nil {
			result.LiveError = inspectErr.Error()
		} else {
			live := maintenance.BuildLiveOperationState(operation, inventory)
			result.Live = &live
		}
	}
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			report(stderr, "write maintenance show output: %v\n", err)
			return ExitDockerConnection
		}
	} else if err := writeMaintenanceShow(stdout, result); err != nil {
		report(stderr, "write maintenance show output: %v\n", err)
		return ExitDockerConnection
	}
	if result.LiveError != "" {
		return ExitDockerConnection
	}
	return ExitSuccess
}

func writeMaintenanceShow(writer io.Writer, result maintenance.ShowResult) error {
	mutation := "no"
	if result.Operation.MutationOccurred {
		mutation = "yes"
	}
	if _, err := fmt.Fprintf(
		writer,
		"Operation: %s\nPhase: %s\nCluster: %s\nTarget: %s (%s) %s\nMutation occurred: %s\n",
		result.Operation.ID,
		result.Operation.Phase,
		result.Operation.ClusterID,
		result.Operation.Target.Hostname,
		result.Operation.Target.ID,
		result.Operation.Target.Role,
		mutation,
	); err != nil {
		return err
	}
	if result.Operation.LastError != "" {
		if _, err := fmt.Fprintf(writer, "Last error: %s\n", result.Operation.LastError); err != nil {
			return err
		}
	}
	if result.LiveError != "" {
		_, err := fmt.Fprintf(writer, "Live state: unavailable: %s\n", result.LiveError)
		return err
	}
	if result.Live == nil {
		return nil
	}
	if !result.Live.ClusterMatchesOperation {
		if _, err := fmt.Fprintf(writer, "Live cluster: %s (operation cluster mismatch)\n", result.Live.ClusterID); err != nil {
			return err
		}
	}
	if result.Live.Target == nil {
		if _, err := io.WriteString(writer, "Live target: missing\n"); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintf(writer, "Live target: %s %s\n", result.Live.Target.State, result.Live.Target.Availability); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "Live target tasks: %d desired-running\n", result.Live.TargetDesiredTasks); err != nil {
		return err
	}
	if len(result.Live.AffectedServices) == 0 {
		return nil
	}
	if _, err := io.WriteString(writer, "\nAffected services:\n"); err != nil {
		return err
	}
	table := tabwriter.NewWriter(writer, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "  SERVICE\tSERVICE ID\tCLASS\tRUNNING/DESIRED\tSTATE")
	for _, service := range result.Live.AffectedServices {
		class := service.Mode
		if service.Singleton {
			class = "singleton"
		}
		state := "unconverged"
		counts := fmt.Sprintf("%d/%d", service.RunningTasks, service.DesiredTasks)
		if !service.Present {
			state = "missing"
			counts = "-"
		} else if service.Converged {
			state = "converged"
		}
		_, _ = fmt.Fprintf(table, "  %s\t%s\t%s\t%s\t%s\n", service.Name, service.ID, class, counts, state)
	}
	return table.Flush()
}

func runMaintenanceBegin(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("maintenance begin", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	timeout := flags.Duration("timeout", 5*time.Minute, "maintenance begin timeout")
	args = normalizeBeginArgs(args)
	if err := flags.Parse(args); err != nil || flags.NArg() != 1 || *timeout <= 0 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] maintenance begin <node> [--timeout duration] [--json]\n")
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
		report(stderr, "Docker connection does not support Swarm node maintenance\n")
		return ExitDockerConnection
	}
	stateDir, err := operations.DefaultStateDir()
	if err != nil {
		report(stderr, "configure operation state: %v\n", err)
		return ExitInvalidUsage
	}
	store := operations.NewStore(stateDir)
	beginner := maintenance.Beginner{
		Client:       maintenanceConnection,
		Store:        store,
		Timeout:      *timeout,
		PollInterval: time.Second,
		Progress: func(operation maintenance.Operation) {
			_, _ = fmt.Fprintf(stderr, "operation %s: %s\n", operation.ID, operation.Phase)
		},
	}
	operation, err := beginner.Begin(ctx, flags.Arg(0))
	if err != nil {
		return reportMaintenanceBeginError(err, stdout, stderr)
	}
	result := struct {
		SchemaVersion int                   `json:"schema_version"`
		Operation     maintenance.Operation `json:"operation"`
	}{SchemaVersion: maintenance.OperationSchemaVersion, Operation: operation}
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			report(stderr, "write maintenance begin output: %v\n", err)
			return ExitDockerConnection
		}
		return ExitSuccess
	}
	_, err = fmt.Fprintf(stdout, "Operation: %s\nTarget: %s (%s)\nPhase: %s\n", operation.ID, operation.Target.Hostname, operation.Target.ID, operation.Phase)
	if err != nil {
		report(stderr, "write maintenance begin output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

func normalizeBeginArgs(args []string) []string {
	var flagArgs []string
	var positional []string
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--json":
			flagArgs = append(flagArgs, args[index])
		case "--timeout":
			flagArgs = append(flagArgs, args[index])
			if index+1 < len(args) {
				index++
				flagArgs = append(flagArgs, args[index])
			}
		default:
			if strings.HasPrefix(args[index], "--timeout=") {
				flagArgs = append(flagArgs, args[index])
			} else {
				positional = append(positional, args[index])
			}
		}
	}
	return append(flagArgs, positional...)
}

func reportMaintenanceBeginError(err error, stdout, stderr io.Writer) int {
	var safety *maintenance.SafetyError
	if errors.As(err, &safety) {
		for _, finding := range safety.Result.Findings {
			if finding.Level == preflight.LevelBlocker {
				_, _ = fmt.Fprintf(stdout, "BLOCKER: %s\n", finding.Message)
			}
		}
		return ExitSafetyGate
	}
	var active *operations.ActiveOperationError
	if errors.As(err, &active) {
		report(stderr, "%v\n", active)
		return ExitSafetyGate
	}
	var beginError *maintenance.BeginError
	if errors.As(err, &beginError) {
		report(stderr, "%v\n", beginError)
		if beginError.MutationOccurred {
			report(stderr, "RECOVERY: node remains drained; inspect operation %s before further mutation\n", beginError.OperationID)
			return ExitPartialMutation
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		report(stderr, "maintenance begin stopped: %v\n", err)
		return ExitTimeout
	}
	report(stderr, "maintenance begin: %v\n", err)
	return ExitDockerConnection
}

func runStatus(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] status [--json]\n")
		}
		return ExitInvalidUsage
	}

	connection, err := connector.Connect(ctx, contextName)
	if err != nil {
		report(stderr, "configure Docker connection: %v\n", err)
		return ExitDockerConnection
	}
	defer func() { _ = connection.Close() }()

	result, err := connection.Inspect(ctx)
	if err != nil {
		report(stderr, "inspect Docker Swarm: %v\n", err)
		return ExitDockerConnection
	}
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			report(stderr, "write status output: %v\n", err)
			return ExitDockerConnection
		}
		return ExitSuccess
	}

	var output strings.Builder
	for _, task := range result.UnhealthyTasks {
		_, _ = fmt.Fprintf(&output, "UNSAFE: task %s is %s", task.Name, task.State)
		if task.Error != "" {
			_, _ = fmt.Fprintf(&output, ": %s", task.Error)
		}
		output.WriteByte('\n')
	}
	for _, service := range result.Services {
		if !service.Converged {
			_, _ = fmt.Fprintf(&output, "UNSAFE: service %s has %d/%d running tasks\n", service.Name, service.RunningTasks, service.DesiredTasks)
		}
	}
	control := "unavailable"
	if result.Cluster.ControlAvailable {
		control = "available"
	} else {
		output.WriteString("UNSAFE: Swarm control is unavailable\n")
	}
	_, _ = fmt.Fprintf(
		&output, "Cluster: %s\nEndpoint: %s\nSwarm: %s\nControl: %s\n",
		result.Cluster.ID,
		result.Endpoint,
		result.Cluster.LocalState,
		control,
	)
	if result.Leader != "" {
		_, _ = fmt.Fprintf(&output, "Leader: %s\n", result.Leader)
	}
	if len(result.Nodes) > 0 {
		output.WriteString("\nNodes:\n")
		table := tabwriter.NewWriter(&output, 0, 2, 2, ' ', 0)
		for _, node := range result.Nodes {
			_, _ = fmt.Fprintf(table, "  %s\t%s\t%s\t%s\t%s", node.Hostname, node.ID, node.Role, node.State, node.Availability)
			if node.ManagerStatus != "" {
				_, _ = fmt.Fprintf(table, "\t%s", node.ManagerStatus)
			}
			_, _ = fmt.Fprintln(table)
		}
		_ = table.Flush()
	}
	if len(result.Services) > 0 {
		output.WriteString("\nServices:\n")
		table := tabwriter.NewWriter(&output, 0, 2, 2, ' ', 0)
		for _, service := range result.Services {
			convergence := "unconverged"
			if service.Converged {
				convergence = "converged"
			}
			_, _ = fmt.Fprintf(
				table,
				"  %s\t%s\t%s\t%d/%d\t%s\n",
				service.Name,
				service.ID,
				service.Mode,
				service.RunningTasks,
				service.DesiredTasks,
				convergence,
			)
		}
		_ = table.Flush()
	}
	if len(result.UnhealthyTasks) > 0 {
		output.WriteString("\nUnhealthy tasks:\n")
		table := tabwriter.NewWriter(&output, 0, 2, 2, ' ', 0)
		for _, task := range result.UnhealthyTasks {
			_, _ = fmt.Fprintf(
				table,
				"  %s\t%s\t%s\t%s\t%s\t%s",
				task.Name,
				task.ID,
				task.Service,
				task.Node,
				task.DesiredState,
				task.State,
			)
			if task.Error != "" {
				_, _ = fmt.Fprintf(table, "\t%s", task.Error)
			}
			_, _ = fmt.Fprintln(table)
		}
		_ = table.Flush()
	}
	_, err = io.WriteString(stdout, output.String())
	if err != nil {
		report(stderr, "write status output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

func runCheck(ctx context.Context, args []string, contextName string, connector status.Connector, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	if len(args) == 2 && args[1] == "--json" {
		args = []string{"--json", args[0]}
	}
	if err := flags.Parse(args); err != nil || flags.NArg() != 1 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] check <node> [--json]\n")
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
	result := preflight.CheckNode(inventory, flags.Arg(0))
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			report(stderr, "write check output: %v\n", err)
			return ExitDockerConnection
		}
		if !result.Safe {
			return ExitSafetyGate
		}
		return ExitSuccess
	}
	for _, finding := range result.Findings {
		if finding.Level != preflight.LevelBlocker {
			continue
		}
		_, _ = fmt.Fprintf(stdout, "%s: %s\n", strings.ToUpper(string(finding.Level)), finding.Message)
	}
	for _, finding := range result.Findings {
		if finding.Level == preflight.LevelBlocker {
			continue
		}
		_, _ = fmt.Fprintf(stdout, "%s: %s\n", strings.ToUpper(string(finding.Level)), finding.Message)
	}
	if !result.Safe {
		targetName := result.RequestedNode
		if result.Target != nil {
			targetName = result.Target.Hostname
		}
		_, _ = fmt.Fprintf(stdout, "UNSAFE: target node %s failed checks\n", targetName)
		writeTargetWorkload(stdout, result.TargetWorkload)
		return ExitSafetyGate
	}
	_, _ = fmt.Fprintf(stdout, "SAFE: target node %s passed checks\n", result.Target.Hostname)
	writeTargetWorkload(stdout, result.TargetWorkload)
	return ExitSuccess
}

func writeTargetWorkload(writer io.Writer, workload *preflight.TargetWorkload) {
	if workload == nil {
		return
	}

	taskNoun := "tasks"
	if workload.DesiredRunningTaskCount == 1 {
		taskNoun = "task"
	}
	serviceNoun := "services"
	if len(workload.AffectedServices) == 1 {
		serviceNoun = "service"
	}
	_, _ = fmt.Fprintf(
		writer,
		"\nTarget workloads: %d desired-running %s across %d affected %s\n",
		workload.DesiredRunningTaskCount,
		taskNoun,
		len(workload.AffectedServices),
		serviceNoun,
	)
	if len(workload.Tasks) == 0 {
		return
	}

	table := tabwriter.NewWriter(writer, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "  TASK NAME\tTASK ID\tSERVICE\tSERVICE ID\tSTATE")
	for _, task := range workload.Tasks {
		_, _ = fmt.Fprintf(table, "  %s\t%s\t%s\t%s\t%s\n", task.Name, task.ID, task.Service, task.ServiceID, task.State)
	}
	_ = table.Flush()

	_, _ = fmt.Fprintln(writer, "\nAffected services:")
	table = tabwriter.NewWriter(writer, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "  SERVICE NAME\tSERVICE ID\tCLASS\tRUNNING/DESIRED")
	for _, service := range workload.AffectedServices {
		class := service.Mode
		if service.Singleton {
			class = "singleton"
		}
		_, _ = fmt.Fprintf(table, "  %s\t%s\t%s\t%d/%d\n", service.Name, service.ID, class, service.RunningTasks, service.DesiredTasks)
	}
	_ = table.Flush()
}

func report(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, format, args...)
}
