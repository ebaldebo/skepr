package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecondSignalTerminatesAfterFirstCancels(t *testing.T) {
	tests := []struct {
		name     string
		signal   os.Signal
		exitCode int
	}{
		{name: "interrupt", signal: os.Interrupt, exitCode: 130},
		{name: "terminate", signal: syscall.SIGTERM, exitCode: 143},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "signal")
			command := exec.Command(os.Args[0], "-test.run=TestSignalHelperProcess", "--", marker)
			require.NoError(t, command.Start())
			t.Cleanup(func() { _ = command.Process.Kill() })
			require.Eventually(t, func() bool {
				_, err := os.Stat(marker + ".ready")
				return err == nil
			}, time.Second, time.Millisecond)

			require.NoError(t, command.Process.Signal(test.signal))
			require.Eventually(t, func() bool {
				_, err := os.Stat(marker + ".canceled")
				return err == nil
			}, time.Second, time.Millisecond)
			require.NoError(t, command.Process.Signal(test.signal))
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			select {
			case err := <-done:
				var exitError *exec.ExitError
				require.ErrorAs(t, err, &exitError)
				assert.Equal(t, test.exitCode, exitError.ExitCode())
			case <-time.After(time.Second):
				t.Fatal("second signal did not terminate the process")
			}
		})
	}
}

func TestSignalHelperProcess(t *testing.T) {
	separator := -1
	for index, argument := range os.Args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator == -1 || len(os.Args) <= separator+1 {
		return
	}
	marker := os.Args[separator+1]
	ctx, stop := signalContext(context.Background(), os.Exit)
	defer stop()
	require.NoError(t, os.WriteFile(marker+".ready", nil, 0o600))
	<-ctx.Done()
	require.NoError(t, os.WriteFile(marker+".canceled", nil, 0o600))
	select {}
}
