//go:build integration

package swarm_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/cli"
	skeprdocker "github.com/ebaldebo/skepr/internal/docker"
	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/api/types/swarm"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthyFiveNodeSwarm(t *testing.T) {
	endpoint := os.Getenv("SKEPR_INTEGRATION_DOCKER_HOST")
	require.NotEmpty(t, endpoint, "run integration/swarm/run to create the disposable Swarm")
	parsedEndpoint, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.Equal(t, "tcp", parsedEndpoint.Scheme)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, parsedEndpoint.Hostname())

	t.Setenv("DOCKER_HOST", endpoint)
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)

	connector := skeprdocker.NewConnector()
	var statusOutput bytes.Buffer
	var statusErrors bytes.Buffer
	exitCode := cli.Run(context.Background(), []string{"status", "--json"}, connector, &statusOutput, &statusErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, statusErrors.String())

	var health status.HealthReport
	require.NoError(t, json.Unmarshal(statusOutput.Bytes(), &health))
	assert.Equal(t, status.HealthSchemaVersion, health.SchemaVersion)
	assert.Equal(t, status.HealthHealthy, health.Health)
	assert.Empty(t, health.Findings)
	assert.Equal(t, 3, health.Summary.HealthyManagers)
	assert.Equal(t, 5, health.Summary.ReadyNodes)
	assert.Equal(t, 5, health.Summary.ActiveNodes)
	inventory := status.Result{Nodes: health.Nodes}
	assert.Len(t, inventory.Nodes, 5)
	managerCount := 0
	workerCount := 0
	for _, node := range inventory.Nodes {
		switch node.Role {
		case "manager":
			managerCount++
		case "worker":
			workerCount++
		}
	}
	assert.Equal(t, 3, managerCount)
	assert.Equal(t, 2, workerCount)

	var checkOutput bytes.Buffer
	var checkErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"check", "worker-1"}, connector, &checkOutput, &checkErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, checkErrors.String())
	assert.Contains(t, checkOutput.String(), "PASS: Swarm manager manager-1 is healthy")
	assert.Contains(t, checkOutput.String(), "PASS: Swarm manager manager-2 is healthy")
	assert.Contains(t, checkOutput.String(), "PASS: Swarm manager manager-3 is healthy")
	assert.Contains(t, checkOutput.String(), "SAFE: target node worker-1 passed checks")
	assert.Contains(t, checkOutput.String(), "Target workloads: 0 desired-running tasks across 0 affected services")

	var beginOutput bytes.Buffer
	var beginErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "begin", "worker-1", "--timeout", "30s"}, connector, &beginOutput, &beginErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, beginErrors.String())
	assert.Contains(t, beginOutput.String(), "Target: worker-1 (")
	assert.Contains(t, beginOutput.String(), "Phase: maintenance-ready")

	connection, err := connector.Connect(context.Background(), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	inventory, err = connection.Inspect(context.Background())
	require.NoError(t, err)
	workerAvailability := ""
	for _, node := range inventory.Nodes {
		if node.Hostname == "worker-1" {
			workerAvailability = node.Availability
		}
	}
	assert.Equal(t, "drain", workerAvailability)

	var showOutput bytes.Buffer
	var showErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "show"}, connector, &showOutput, &showErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, showErrors.String())
	assert.Contains(t, showOutput.String(), "Phase: maintenance-ready")
	assert.Contains(t, showOutput.String(), "Live target: ready drain")
	assert.Contains(t, showOutput.String(), "Live target tasks: 0 desired-running")
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	operation, err := store.LatestActive()
	require.NoError(t, err)
	require.NotNil(t, operation)

	var finishOutput bytes.Buffer
	var finishErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "finish", operation.ID, "--timeout", "30s"}, connector, &finishOutput, &finishErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, finishErrors.String())
	assert.Contains(t, finishOutput.String(), "Phase: completed")

	inventory, err = connection.Inspect(context.Background())
	require.NoError(t, err)
	for _, node := range inventory.Nodes {
		if node.Hostname == "worker-1" {
			assert.Equal(t, "active", node.Availability)
		}
	}
	persisted, err := store.Load(operation.ID)
	require.NoError(t, err)
	assert.Equal(t, maintenance.PhaseCompleted, persisted.Phase)

	var secondBeginOutput bytes.Buffer
	var secondBeginErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "begin", "worker-2", "--timeout", "30s"}, connector, &secondBeginOutput, &secondBeginErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, secondBeginErrors.String())
	operation, err = store.LatestActive()
	require.NoError(t, err)
	require.NotNil(t, operation)
	assert.Equal(t, "worker-2", operation.Target.Hostname)

	engine, err := mobyclient.New(mobyclient.WithHost(endpoint), mobyclient.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	replicas := uint64(1)
	created, err := engine.ServiceCreate(context.Background(), mobyclient.ServiceCreateOptions{Spec: swarm.ServiceSpec{
		Annotations: swarm.Annotations{Name: "skepr-stalled-singleton"},
		TaskTemplate: swarm.TaskSpec{
			ContainerSpec: &swarm.ContainerSpec{Image: "alpine:3.20"},
			Placement:     &swarm.Placement{Constraints: []string{"node.hostname==missing-node"}},
		},
		Mode: swarm.ServiceMode{Replicated: &swarm.ReplicatedService{Replicas: &replicas}},
	}})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = engine.ServiceRemove(context.Background(), created.ID, mobyclient.ServiceRemoveOptions{})
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		inventory, err = connection.Inspect(context.Background())
		require.NoError(t, err)
		ready := false
		for _, service := range inventory.Services {
			if service.ID == created.ID && service.RunningTasks == 0 && service.DesiredTasks == 1 {
				ready = true
			}
		}
		if ready {
			break
		}
		require.True(t, time.Now().Before(deadline), "stalled singleton did not reach 0/1")
		time.Sleep(100 * time.Millisecond)
	}
	var diagnosisOutput bytes.Buffer
	var diagnosisErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"service", "diagnose", "skepr-stalled-singleton", "--json"}, connector, &diagnosisOutput, &diagnosisErrors)
	require.Equal(t, cli.ExitSafetyGate, exitCode, diagnosisErrors.String())
	var diagnosis status.ServiceDiagnosis
	require.NoError(t, json.Unmarshal(diagnosisOutput.Bytes(), &diagnosis))
	assert.Equal(t, status.ServiceDiagnosisSchemaVersion, diagnosis.SchemaVersion)
	assert.Equal(t, status.HealthDegraded, diagnosis.Health)
	assert.Equal(t, created.ID, diagnosis.Service.ID)
	assert.Equal(t, uint64(0), diagnosis.Service.RunningTasks)
	assert.Equal(t, uint64(1), diagnosis.Service.DesiredTasks)
	assert.Empty(t, diagnosis.CurrentFailures)

	operation.Phase = maintenance.PhaseWaitingServices
	operation.PhaseTimestamps[maintenance.PhaseWaitingServices] = time.Now().UTC()
	operation.TargetWorkload = preflight.TargetWorkload{AffectedServices: []preflight.AffectedService{
		{ID: created.ID, Name: "skepr-stalled-singleton", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Singleton: true},
	}}
	require.NoError(t, store.Save(*operation))
	beforeReconcile, err := engine.ServiceInspect(context.Background(), created.ID, mobyclient.ServiceInspectOptions{})
	require.NoError(t, err)

	var reconcileOutput bytes.Buffer
	var reconcileErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "reconcile", operation.ID, "--timeout", "1s"}, connector, &reconcileOutput, &reconcileErrors)
	require.Equal(t, cli.ExitPartialMutation, exitCode, reconcileErrors.String())
	assert.Empty(t, reconcileOutput.String())
	assert.Contains(t, reconcileErrors.String(), "RECOVERY: node remains drained")

	updated, err := engine.ServiceInspect(context.Background(), created.ID, mobyclient.ServiceInspectOptions{})
	require.NoError(t, err)
	assert.Greater(t, updated.Service.Version.Index, beforeReconcile.Service.Version.Index, reconcileErrors.String())
	assert.Greater(t, updated.Service.Spec.TaskTemplate.ForceUpdate, beforeReconcile.Service.Spec.TaskTemplate.ForceUpdate, reconcileErrors.String())
	persisted, err = store.Load(operation.ID)
	require.NoError(t, err)
	assert.Equal(t, maintenance.PhaseReconciling, persisted.Phase)
	require.Len(t, persisted.ReconciliationAttempts, 1)
	assert.Equal(t, maintenance.ReconciliationStarted, persisted.ReconciliationAttempts[0].Result)
	assert.Nil(t, persisted.ReconciliationAttempts[0].CompletedAt)
	require.NotNil(t, persisted.ReconciliationAttempts[0].ForceUpdateBefore)
	assert.Equal(t, beforeReconcile.Service.Spec.TaskTemplate.ForceUpdate, *persisted.ReconciliationAttempts[0].ForceUpdateBefore)

	zero := uint64(0)
	recoverySpec := updated.Service.Spec
	recoverySpec.Mode.Replicated.Replicas = &zero
	_, err = engine.ServiceUpdate(context.Background(), created.ID, mobyclient.ServiceUpdateOptions{
		Version:          updated.Service.Version,
		Spec:             recoverySpec,
		RegistryAuthFrom: swarm.RegistryAuthFromSpec,
	})
	require.NoError(t, err)
	forceUpdateAfterTimeout := updated.Service.Spec.TaskTemplate.ForceUpdate

	var resumeReconcileOutput bytes.Buffer
	var resumeReconcileErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "reconcile", operation.ID, "--timeout", "30s"}, connector, &resumeReconcileOutput, &resumeReconcileErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, resumeReconcileErrors.String())
	assert.Contains(t, resumeReconcileOutput.String(), "Phase: maintenance-ready")
	resumedService, err := engine.ServiceInspect(context.Background(), created.ID, mobyclient.ServiceInspectOptions{})
	require.NoError(t, err)
	assert.Equal(t, forceUpdateAfterTimeout, resumedService.Service.Spec.TaskTemplate.ForceUpdate)

	var recoveredFinishOutput bytes.Buffer
	var recoveredFinishErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"maintenance", "finish", operation.ID, "--timeout", "30s"}, connector, &recoveredFinishOutput, &recoveredFinishErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, recoveredFinishErrors.String())
	assert.Contains(t, recoveredFinishOutput.String(), "Phase: completed")
}
