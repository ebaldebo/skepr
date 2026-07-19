package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ebaldebo/skepr/internal/cli"
	skeprdocker "github.com/ebaldebo/skepr/internal/docker"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signalContext(context.Background(), os.Exit)
	defer stop()

	return cli.RunWithInput(ctx, args, skeprdocker.NewConnector(), os.Stdin, stdout, stderr)
}

func signalContext(parent context.Context, exit func(int)) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	stopped := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			signal.Stop(signals)
			close(stopped)
			cancel()
		})
	}
	go func() {
		select {
		case <-parent.Done():
			cancel()
			return
		case <-stopped:
			return
		case <-signals:
			cancel()
		}
		select {
		case <-stopped:
		case received := <-signals:
			exit(signalExitCode(received))
		}
	}()
	return ctx, stop
}

func signalExitCode(received os.Signal) int {
	switch received {
	case os.Interrupt:
		return 130
	case syscall.SIGTERM:
		return 143
	default:
		return 1
	}
}
