package main

import (
	"context"
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

	return cli.Run(ctx, args, skeprdocker.NewConnector(), stdout, stderr)
}
