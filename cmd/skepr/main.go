package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ebaldebo/skepr/internal/cli"
	skeprdocker "github.com/ebaldebo/skepr/internal/docker"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	inspector, err := skeprdocker.NewInspector()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Docker connection: %v\n", err)
		return cli.ExitDockerConnection
	}
	defer func() { _ = inspector.Close() }()

	return cli.Run(ctx, args, inspector, stdout, stderr)
}
