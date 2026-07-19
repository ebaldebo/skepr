package maintenance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadPlanDecodesArgvHooksAndDockerContexts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "maintenance.toml")
	require.NoError(t, os.WriteFile(path, []byte(`[target]
hostname = "manager-2"

[swarm]
contexts = ["manager-1", "manager-3"]
endpoints = ["ssh://root@10.0.0.2", "ssh://root@10.0.0.3"]

[commands]
pre = ["ssh", "manager-2", "hostname"]
update = ["nixos-rebuild", "switch", "--flake", ".#manager-2"]
verify = ["ssh", "manager-2", "docker", "info"]
`), 0o600))

	plan, err := LoadPlan(path)

	require.NoError(t, err)
	assert.Equal(t, "manager-2", plan.Target.Hostname)
	assert.Equal(t, []string{"manager-1", "manager-3"}, plan.Swarm.Contexts)
	assert.Equal(t, []string{"ssh://root@10.0.0.2", "ssh://root@10.0.0.3"}, plan.Swarm.Endpoints)
	assert.Equal(t, []string{"ssh", "manager-2", "hostname"}, plan.Commands.Pre)
	assert.Equal(t, []string{"nixos-rebuild", "switch", "--flake", ".#manager-2"}, plan.Commands.Update)
	assert.Equal(t, []string{"ssh", "manager-2", "docker", "info"}, plan.Commands.Verify)
}

func TestLoadPlanRejectsUnknownInfrastructurePolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "maintenance.toml")
	require.NoError(t, os.WriteFile(path, []byte(`[target]
hostname = "worker-1"
mount = "/srv/data"

[commands]
update = ["true"]
`), 0o600))

	_, err := LoadPlan(path)

	assert.ErrorContains(t, err, "unknown key target.mount")
}

func TestLoadPlanAllowsCommandOnlyReusablePlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "maintenance.toml")
	require.NoError(t, os.WriteFile(path, []byte(`[commands]
pre = ["pre-check"]
update = ["update-host"]
verify = ["verify-host"]
`), 0o600))

	plan, err := LoadPlan(path)

	require.NoError(t, err)
	assert.Empty(t, plan.Target.Hostname)
	assert.Equal(t, []string{"update-host"}, plan.Commands.Update)
}
