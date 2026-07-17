package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

const (
	ExitSuccess          = 0
	ExitSafetyGate       = 1
	ExitInvalidUsage     = 2
	ExitDockerConnection = 3
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
	default:
		report(stderr, "usage: skepr [--context name] <command>\n")
		return ExitInvalidUsage
	}
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
		return ExitSafetyGate
	}
	_, _ = fmt.Fprintf(stdout, "SAFE: target node %s passed checks\n", result.Target.Hostname)
	return ExitSuccess
}

func report(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, format, args...)
}
