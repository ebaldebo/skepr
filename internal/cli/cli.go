package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/ebaldebo/skepr/internal/status"
)

const (
	ExitSuccess          = 0
	ExitInvalidUsage     = 2
	ExitDockerConnection = 3
)

func Run(ctx context.Context, args []string, inspector status.Inspector, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "status" {
		report(stderr, "usage: skepr status [--json]\n")
		return ExitInvalidUsage
	}

	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "emit JSON output")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		if err == nil {
			report(stderr, "usage: skepr status [--json]\n")
		}
		return ExitInvalidUsage
	}

	result, err := inspector.Inspect(ctx)
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

	control := "unavailable"
	if result.Cluster.ControlAvailable {
		control = "available"
	} else {
		if _, err := fmt.Fprintln(stdout, "UNSAFE: Swarm control is unavailable"); err != nil {
			report(stderr, "write status output: %v\n", err)
			return ExitDockerConnection
		}
	}
	_, err = fmt.Fprintf(
		stdout, "Cluster: %s\nEndpoint: %s\nSwarm: %s\nControl: %s\n",
		result.Cluster.ID,
		result.Endpoint,
		result.Cluster.LocalState,
		control,
	)
	if err != nil {
		report(stderr, "write status output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

func report(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, format, args...)
}
