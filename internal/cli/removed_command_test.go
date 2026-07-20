package cli

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaintenanceRunIsNotSupported(t *testing.T) {
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "run"}, maintenanceConnectorError{}, &bytes.Buffer{}, &stderr)

	assert.Equal(t, ExitInvalidUsage, exitCode)
	assert.Equal(t, "usage: skepr [--context name] maintenance <command>\n", stderr.String())
}

func TestLegacyCheckCommandIsNotSupported(t *testing.T) {
	connector := &fakeConnector{contextName: "not-called"}
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"check", "worker-1"}, connector, &bytes.Buffer{}, &stderr)

	assert.Equal(t, ExitInvalidUsage, exitCode)
	assert.Equal(t, "not-called", connector.contextName)
	assert.Equal(t, "usage: skepr [--context name] <command>\n", stderr.String())
}

func TestMaintenanceBeginIsReplacedByNodeCommands(t *testing.T) {
	connector := &fakeConnector{contextName: "not-called"}
	var stderr bytes.Buffer

	exitCode := Run(context.Background(), []string{"maintenance", "begin", "worker-1"}, connector, &bytes.Buffer{}, &stderr)

	assert.Equal(t, ExitInvalidUsage, exitCode)
	assert.Equal(t, "not-called", connector.contextName)
	assert.Equal(t, "maintenance begin is no longer supported; use node drain and node activate\n", stderr.String())
}
