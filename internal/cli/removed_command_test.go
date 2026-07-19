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
