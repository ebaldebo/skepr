package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
)

type maintenanceRunOptions struct {
	target           string
	planPath         string
	resumeID         string
	abortID          string
	preCommand       []string
	updateCommand    []string
	verifyCommand    []string
	managerContexts  []string
	managerEndpoints []string
	preCommandSet    bool
	updateSet        bool
	verifySet        bool
	dashCommand      bool
	timeout          time.Duration
	returnTimeout    time.Duration
	jsonOutput       bool
	resolution       maintenance.CommandResolution
}

func runMaintenanceTransaction(ctx context.Context, args []string, contextName string, connector status.Connector, stdin io.Reader, stdout, stderr io.Writer) int {
	options, err := parseMaintenanceRunArgs(args)
	if err != nil {
		report(stderr, "%v\nusage:\n  skepr [--context name] maintenance run <node> [options] -- <update-command> [args...]\n  skepr [--context name] maintenance run <node> [options] --pre-command <argv...> --update-command <argv...> [--verify-command <argv...>]\n  skepr [--context name] maintenance run <node> [options] --plan <path|->\n  skepr maintenance run --resume <operation-id> [--retry-command | --accept-command]\n  skepr maintenance run --abort <operation-id>\noptions must precede command sections; repeat --manager-context or --manager-endpoint for failover\n", err)
		return ExitInvalidUsage
	}
	stateDir, err := operations.DefaultStateDir()
	if err != nil {
		report(stderr, "configure operation state: %v\n", err)
		return ExitInvalidUsage
	}
	store := operations.NewStore(stateDir)
	if options.abortID != "" {
		operation, abortErr := (maintenance.TransactionRunner{Store: store}).Abort(options.abortID)
		if abortErr != nil {
			return reportMaintenanceRunError(abortErr, stdout, stderr)
		}
		return writeMaintenanceRunResult(operation, options.jsonOutput, stdout, stderr)
	}

	var commands maintenance.RunCommands
	var contexts []string
	var endpoints []string
	if options.resumeID != "" {
		operation, loadErr := store.Load(options.resumeID)
		if loadErr != nil {
			report(stderr, "load maintenance run: %v\n", loadErr)
			return ExitInvalidUsage
		}
		if operation.Run == nil {
			report(stderr, "operation %s was not created by maintenance run\n", operation.ID)
			return ExitInvalidUsage
		}
		contexts = append(contexts, operation.Run.DockerContexts...)
		endpoints = append(endpoints, operation.Run.DockerEndpoints...)
	} else if options.planPath != "" {
		var plan maintenance.Plan
		var loadErr error
		if options.planPath == "-" {
			plan, loadErr = maintenance.LoadPlanReader(stdin)
		} else {
			plan, loadErr = maintenance.LoadPlan(options.planPath)
		}
		if loadErr != nil {
			report(stderr, "%v\n", loadErr)
			return ExitInvalidUsage
		}
		if plan.Target.Hostname != "" && plan.Target.Hostname != options.target {
			report(stderr, "maintenance plan target %s does not match requested node %s\n", plan.Target.Hostname, options.target)
			return ExitInvalidUsage
		}
		commands = plan.Commands
		contexts = append(contexts, plan.Swarm.Contexts...)
		endpoints = append(endpoints, plan.Swarm.Endpoints...)
	} else {
		commands = maintenance.RunCommands{
			Pre:    append([]string(nil), options.preCommand...),
			Update: append([]string(nil), options.updateCommand...),
			Verify: append([]string(nil), options.verifyCommand...),
		}
	}
	contexts = append(contexts, options.managerContexts...)
	endpoints = append(endpoints, options.managerEndpoints...)
	if contextName != "" {
		contexts = append([]string{contextName}, contexts...)
	}
	contexts = uniqueContexts(contexts)
	endpoints = uniqueContexts(endpoints)
	if len(contexts) == 0 && len(endpoints) == 0 {
		contexts = []string{""}
	}

	pool := maintenance.NewEndpointPoolWithEndpoints(connector, contexts, endpoints)
	defer func() { _ = pool.Close() }()
	runner := maintenance.TransactionRunner{
		Client:        pool,
		Store:         store,
		CommandOutput: stderr,
		Timeout:       options.timeout,
		ReturnTimeout: options.returnTimeout,
		PollInterval:  time.Second,
		Progress: func(operation maintenance.Operation) {
			if operation.Run != nil && operation.Run.Phase != "" {
				_, _ = fmt.Fprintf(stderr, "operation %s: %s (%s)\n", operation.ID, operation.Phase, operation.Run.Phase)
				return
			}
			_, _ = fmt.Fprintf(stderr, "operation %s: %s\n", operation.ID, operation.Phase)
		},
	}
	var operation maintenance.Operation
	if options.resumeID != "" {
		operation, err = runner.Resume(ctx, options.resumeID, options.resolution)
	} else {
		operation, err = runner.Run(ctx, options.target, commands, contexts, endpoints)
	}
	if err != nil {
		return reportMaintenanceRunError(err, stdout, stderr)
	}
	return writeMaintenanceRunResult(operation, options.jsonOutput, stdout, stderr)
}

func writeMaintenanceRunResult(operation maintenance.Operation, jsonOutput bool, stdout, stderr io.Writer) int {
	result := struct {
		SchemaVersion int                   `json:"schema_version"`
		Operation     maintenance.Operation `json:"operation"`
	}{SchemaVersion: maintenance.OperationSchemaVersion, Operation: operation}
	if jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(result); err != nil {
			report(stderr, "write maintenance run output: %v\n", err)
			return ExitDockerConnection
		}
		return ExitSuccess
	}
	if _, err := fmt.Fprintf(stdout, "Operation: %s\nTarget: %s (%s)\nPhase: %s\n", operation.ID, operation.Target.Hostname, operation.Target.ID, operation.Phase); err != nil {
		report(stderr, "write maintenance run output: %v\n", err)
		return ExitDockerConnection
	}
	return ExitSuccess
}

func parseMaintenanceRunArgs(args []string) (maintenanceRunOptions, error) {
	options := maintenanceRunOptions{timeout: 5 * time.Minute, returnTimeout: 30 * time.Minute}
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if argument == "--" {
			if options.updateSet || options.dashCommand {
				return options, fmt.Errorf("update command was provided more than once")
			}
			if index+1 >= len(args) {
				return options, fmt.Errorf("-- requires an update command")
			}
			options.dashCommand = true
			options.updateCommand = append([]string(nil), args[index+1:]...)
			break
		}
		switch {
		case isMaintenanceRunCommandSection(argument):
			command, next := maintenanceRunCommandArgs(args, index+1)
			if len(command) == 0 {
				return options, fmt.Errorf("%s requires at least one argument", argument)
			}
			switch argument {
			case "--pre-command":
				if options.preCommandSet {
					return options, fmt.Errorf("--pre-command was provided more than once")
				}
				options.preCommandSet = true
				options.preCommand = command
			case "--update-command":
				if options.updateSet || options.dashCommand {
					return options, fmt.Errorf("update command was provided more than once")
				}
				options.updateSet = true
				options.updateCommand = command
			case "--verify-command":
				if options.verifySet {
					return options, fmt.Errorf("--verify-command was provided more than once")
				}
				options.verifySet = true
				options.verifyCommand = command
			}
			index = next - 1
		case argument == "--json":
			options.jsonOutput = true
		case argument == "--retry-command":
			if options.resolution != "" {
				return options, fmt.Errorf("command resolution options are mutually exclusive")
			}
			options.resolution = maintenance.CommandResolutionRetry
		case argument == "--accept-command":
			if options.resolution != "" {
				return options, fmt.Errorf("command resolution options are mutually exclusive")
			}
			options.resolution = maintenance.CommandResolutionAccept
		case argument == "--plan" || argument == "--resume" || argument == "--abort" || argument == "--timeout" || argument == "--return-timeout" || argument == "--manager-context" || argument == "--manager-endpoint":
			if index+1 >= len(args) {
				return options, fmt.Errorf("%s requires a value", argument)
			}
			index++
			if err := setMaintenanceRunOption(&options, argument, args[index]); err != nil {
				return options, err
			}
		case strings.HasPrefix(argument, "--plan="):
			options.planPath = strings.TrimPrefix(argument, "--plan=")
		case strings.HasPrefix(argument, "--resume="):
			options.resumeID = strings.TrimPrefix(argument, "--resume=")
		case strings.HasPrefix(argument, "--abort="):
			options.abortID = strings.TrimPrefix(argument, "--abort=")
		case strings.HasPrefix(argument, "--manager-context="):
			if err := setMaintenanceRunOption(&options, "--manager-context", strings.TrimPrefix(argument, "--manager-context=")); err != nil {
				return options, err
			}
		case strings.HasPrefix(argument, "--manager-endpoint="):
			if err := setMaintenanceRunOption(&options, "--manager-endpoint", strings.TrimPrefix(argument, "--manager-endpoint=")); err != nil {
				return options, err
			}
		case strings.HasPrefix(argument, "--timeout="):
			if err := setMaintenanceRunOption(&options, "--timeout", strings.TrimPrefix(argument, "--timeout=")); err != nil {
				return options, err
			}
		case strings.HasPrefix(argument, "--return-timeout="):
			if err := setMaintenanceRunOption(&options, "--return-timeout", strings.TrimPrefix(argument, "--return-timeout=")); err != nil {
				return options, err
			}
		case strings.HasPrefix(argument, "-"):
			return options, fmt.Errorf("unknown maintenance run option %s", argument)
		case options.target == "":
			options.target = argument
		default:
			return options, fmt.Errorf("unexpected maintenance run argument %s", strconv.Quote(argument))
		}
	}
	if options.resumeID != "" {
		if options.target != "" || options.planPath != "" || options.abortID != "" || options.hasCommands() || options.hasManagerEndpoints() {
			return options, fmt.Errorf("--resume cannot be combined with a node, plan or update command")
		}
		return options, nil
	}
	if options.abortID != "" {
		if options.target != "" || options.planPath != "" || options.hasCommands() || options.hasManagerEndpoints() || options.resolution != "" {
			return options, fmt.Errorf("--abort cannot be combined with a node, plan, update command or command resolution")
		}
		return options, nil
	}
	if options.resolution != "" {
		return options, fmt.Errorf("command resolution requires --resume")
	}
	if options.target == "" {
		return options, fmt.Errorf("maintenance run requires a target node")
	}
	inlineCommands := options.preCommandSet || options.updateSet || options.verifySet
	if inlineCommands && !options.updateSet {
		return options, fmt.Errorf("inline hooks require --update-command")
	}
	sources := 0
	if options.planPath != "" {
		sources++
	}
	if options.dashCommand {
		sources++
	}
	if inlineCommands {
		sources++
	}
	if sources != 1 {
		return options, fmt.Errorf("maintenance run requires exactly one command source: --plan, -- <update-command> or inline command hooks")
	}
	return options, nil
}

func (o maintenanceRunOptions) hasCommands() bool {
	return o.dashCommand || o.preCommandSet || o.updateSet || o.verifySet
}

func (o maintenanceRunOptions) hasManagerEndpoints() bool {
	return len(o.managerContexts) > 0 || len(o.managerEndpoints) > 0
}

func isMaintenanceRunCommandSection(argument string) bool {
	return argument == "--pre-command" || argument == "--update-command" || argument == "--verify-command"
}

func maintenanceRunCommandArgs(args []string, start int) ([]string, int) {
	end := start
	for end < len(args) && !isMaintenanceRunCommandSection(args[end]) {
		end++
	}
	return append([]string(nil), args[start:end]...), end
}

func setMaintenanceRunOption(options *maintenanceRunOptions, name, value string) error {
	switch name {
	case "--plan":
		options.planPath = value
	case "--resume":
		options.resumeID = value
	case "--abort":
		options.abortID = value
	case "--manager-context":
		if value == "" {
			return fmt.Errorf("--manager-context requires a non-empty value")
		}
		options.managerContexts = append(options.managerContexts, value)
	case "--manager-endpoint":
		if err := maintenance.ValidateManagerEndpoint(value); err != nil {
			return fmt.Errorf("--manager-endpoint %w", err)
		}
		options.managerEndpoints = append(options.managerEndpoints, value)
	case "--timeout", "--return-timeout":
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return fmt.Errorf("%s requires a positive duration", name)
		}
		if name == "--timeout" {
			options.timeout = duration
		} else {
			options.returnTimeout = duration
		}
	}
	return nil
}

func reportMaintenanceRunError(err error, stdout, stderr io.Writer) int {
	var running *maintenance.OperationAlreadyRunningError
	if errors.As(err, &running) {
		report(stderr, "%v\n", running)
		return ExitSafetyGate
	}
	var abortSafety *maintenance.AbortSafetyError
	if errors.As(err, &abortSafety) {
		report(stderr, "maintenance run abort blocked: %v\n", abortSafety)
		return ExitSafetyGate
	}
	var ambiguous *maintenance.AmbiguousCommandError
	if errors.As(err, &ambiguous) {
		report(stderr, "%v\n", ambiguous)
		if ambiguous.Hook == "pre" {
			report(stderr, "RECOVERY: no Swarm mutation occurred; inspect the target, then choose exactly one:\n")
		} else {
			report(stderr, "RECOVERY: node remains drained; inspect the target, then choose exactly one:\n")
		}
		report(stderr, "  skepr maintenance run --resume %s --retry-command\n", ambiguous.OperationID)
		report(stderr, "  skepr maintenance run --resume %s --accept-command\n", ambiguous.OperationID)
		if ambiguous.Hook == "pre" {
			report(stderr, "  skepr maintenance run --abort %s\n", ambiguous.OperationID)
		}
		return ExitPartialMutation
	}
	var safety *maintenance.SafetyError
	if errors.As(err, &safety) {
		for _, finding := range safety.Result.Findings {
			if finding.Level == preflight.LevelBlocker {
				_, _ = fmt.Fprintf(stdout, "BLOCKER: %s\n", finding.Message)
			}
		}
		var transactionError *maintenance.TransactionError
		if errors.As(err, &transactionError) && transactionError.OperationID != "" && !transactionError.Drained && !transactionError.ActivationStarted {
			report(stderr, "RECOVERY: no Swarm mutation occurred; resume with: skepr maintenance run --resume %s\n", transactionError.OperationID)
			report(stderr, "ABORT: skepr maintenance run --abort %s\n", transactionError.OperationID)
		}
		return ExitSafetyGate
	}
	var transactionError *maintenance.TransactionError
	if errors.As(err, &transactionError) {
		report(stderr, "%v\n", transactionError)
		if transactionError.Drained {
			report(stderr, "RECOVERY: node remains drained; resume with: skepr maintenance run --resume %s\n", transactionError.OperationID)
			return ExitPartialMutation
		}
		if transactionError.ActivationStarted {
			report(stderr, "RECOVERY: node activation may have occurred; resume with: skepr maintenance run --resume %s; never redrain automatically\n", transactionError.OperationID)
			return ExitPartialMutation
		}
		if transactionError.OperationID != "" {
			report(stderr, "RECOVERY: no Swarm mutation occurred; resume with: skepr maintenance run --resume %s\n", transactionError.OperationID)
			report(stderr, "ABORT: skepr maintenance run --abort %s\n", transactionError.OperationID)
			return ExitSafetyGate
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ExitTimeout
	}
	report(stderr, "maintenance run: %v\n", err)
	return ExitDockerConnection
}

func uniqueContexts(contexts []string) []string {
	seen := make(map[string]struct{}, len(contexts))
	unique := make([]string, 0, len(contexts))
	for _, contextName := range contexts {
		if _, exists := seen[contextName]; exists {
			continue
		}
		seen[contextName] = struct{}{}
		unique = append(unique, contextName)
	}
	return unique
}
