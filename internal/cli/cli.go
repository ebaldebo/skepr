package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/ebaldebo/skepr/internal/status"
)

const (
	ExitSuccess          = 0
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

	if len(args) == 0 || args[0] != "status" {
		report(stderr, "usage: skepr [--context name] status [--json]\n")
		return ExitInvalidUsage
	}

	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		if err == nil {
			report(stderr, "usage: skepr [--context name] status [--json]\n")
		}
		return ExitInvalidUsage
	}

	connection, err := connector.Connect(ctx, *contextName)
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
	_, err = io.WriteString(stdout, output.String())
	if err != nil {
		report(stderr, "write status output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

func report(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, format, args...)
}
