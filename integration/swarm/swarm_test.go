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

	"github.com/ebaldebo/skepr/internal/activate"
	"github.com/ebaldebo/skepr/internal/cli"
	skeprdocker "github.com/ebaldebo/skepr/internal/docker"
	"github.com/ebaldebo/skepr/internal/drain"
	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/operations"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/rebalance"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/moby/moby/api/types/mount"
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

	var rebalanceOutput bytes.Buffer
	var rebalanceErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"rebalance", "report", "--json"}, connector, &rebalanceOutput, &rebalanceErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, rebalanceErrors.String())
	var rebalanceReport rebalance.Report
	require.NoError(t, json.Unmarshal(rebalanceOutput.Bytes(), &rebalanceReport))
	assert.Equal(t, rebalance.SchemaVersion, rebalanceReport.SchemaVersion)
	assert.Equal(t, health.Cluster.ID, rebalanceReport.ClusterID)
	assert.Zero(t, rebalanceReport.Summary.Opportunities)
	assert.Zero(t, rebalanceReport.Summary.ActiveTasks)
	assert.Zero(t, rebalanceReport.Summary.TasksWithoutCPUReservations)
	assert.Zero(t, rebalanceReport.Summary.TasksWithoutMemoryReservations)
	require.Len(t, rebalanceReport.NodeReservations, 5)
	for _, node := range rebalanceReport.NodeReservations {
		assert.Positive(t, node.Resources.Capacity.NanoCPUs, node.Hostname)
		assert.Positive(t, node.Resources.Capacity.MemoryBytes, node.Hostname)
		assert.Equal(t, node.Resources.Capacity, node.Resources.Available, node.Hostname)
		assert.Empty(t, node.TasksWithoutCPUReservations, node.Hostname)
		assert.Empty(t, node.TasksWithoutMemoryReservations, node.Hostname)
	}

	var previewOutput bytes.Buffer
	var previewErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"node", "drain", "worker-1", "--dry-run", "--json"}, connector, &previewOutput, &previewErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, previewErrors.String())
	var preview drain.Preview
	require.NoError(t, json.Unmarshal(previewOutput.Bytes(), &preview))
	assert.Equal(t, drain.PreviewSchemaVersion, preview.SchemaVersion)
	require.NotNil(t, preview.Target)
	assert.Equal(t, "worker-1", preview.Target.Hostname)
	assert.True(t, preview.SafeToDrain)
	assert.True(t, preview.SafeToTakeOffline)
	assert.Empty(t, preview.ReplicatedTasks)
	assert.Empty(t, preview.GlobalTasks)
	assert.Empty(t, preview.ServiceImpacts)

	var drainOutput bytes.Buffer
	var drainErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"node", "drain", "worker-1", "--timeout", "30s", "--json"}, connector, &drainOutput, &drainErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, drainErrors.String())
	var drainResult drain.Result
	require.NoError(t, json.Unmarshal(drainOutput.Bytes(), &drainResult))
	assert.Equal(t, drain.ResultSchemaVersion, drainResult.SchemaVersion)
	assert.Equal(t, drain.PhaseDrained, drainResult.Phase)
	assert.Equal(t, "worker-1", drainResult.Target.Hostname)
	assert.Equal(t, "drain", drainResult.Availability)
	assert.True(t, drainResult.Evacuated)
	assert.True(t, drainResult.ServicesConverged)

	var activateOutput bytes.Buffer
	var activateErrors bytes.Buffer
	exitCode = cli.Run(context.Background(), []string{"node", "activate", "worker-1", "--timeout", "30s", "--json"}, connector, &activateOutput, &activateErrors)
	require.Equal(t, cli.ExitSuccess, exitCode, activateErrors.String())
	var activateResult activate.Result
	require.NoError(t, json.Unmarshal(activateOutput.Bytes(), &activateResult))
	assert.Equal(t, activate.ResultSchemaVersion, activateResult.SchemaVersion)
	assert.Equal(t, activate.PhaseActive, activateResult.Phase)
	assert.Equal(t, "worker-1", activateResult.Target.Hostname)
	assert.Equal(t, "active", activateResult.Availability)
	assert.Equal(t, activateResult.TotalManagers, activateResult.HealthyManagers)
	assert.Equal(t, activateResult.TotalServices, activateResult.ConvergedServices)

	recoveryDrain := drainNode(t, connector, "worker-1")
	store := operations.NewStore(filepath.Join(stateHome, "skepr"))
	legacyOperation := newLegacyOperation("legacy-worker-1", recoveryDrain)
	require.NoError(t, store.Save(legacyOperation))

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

	secondDrain := drainNode(t, connector, "worker-2")
	require.NoError(t, store.Save(newLegacyOperation("legacy-worker-2", secondDrain)))
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
			ContainerSpec: &swarm.ContainerSpec{Image: "alpine:3.20", Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: "/srv/skepr-test", Target: "/config"},
				{Type: mount.TypeVolume, Source: "skepr-test-data", Target: "/data"},
			}},
			Resources: &swarm.ResourceRequirements{
				Reservations: &swarm.Resources{MemoryBytes: 1 << 60},
			},
			Placement: &swarm.Placement{
				Constraints: []string{"node.hostname==missing-node"},
				Platforms:   []swarm.Platform{{OS: "windows"}},
				MaxReplicas: 1,
			},
		},
		Mode: swarm.ServiceMode{Replicated: &swarm.ReplicatedService{Replicas: &replicas}},
		EndpointSpec: &swarm.EndpointSpec{Ports: []swarm.PortConfig{{
			Protocol:      "tcp",
			TargetPort:    80,
			PublishedPort: 45678,
			PublishMode:   swarm.PortConfigPublishModeHost,
		}}},
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
	assert.Equal(t, []string{"node_readiness", "node_availability", "placement_constraints", "platform_requirements", "cpu_memory_reservations", "maximum_replicas_per_node", "host_published_port_conflicts", "storage_portability_warnings"}, diagnosis.PlacementEligibility.EvaluatedInputs)
	assert.Equal(t, []string{"generic_resources"}, diagnosis.PlacementEligibility.UnevaluatedInputs)
	assert.Equal(t, []string{"node.hostname==missing-node"}, diagnosis.PlacementEligibility.EvaluatedConstraints)
	assert.Equal(t, []status.Platform{{OS: "windows"}}, diagnosis.PlacementEligibility.RequiredPlatforms)
	assert.Equal(t, status.Resources{MemoryBytes: 1 << 60}, diagnosis.PlacementEligibility.RequiredResources)
	assert.Equal(t, uint64(1), diagnosis.PlacementEligibility.MaxReplicasPerNode)
	assert.Equal(t, []status.HostPort{{Protocol: "tcp", PublishedPort: 45678}}, diagnosis.PlacementEligibility.RequiredHostPorts)
	assert.Equal(t, []status.StorageWarning{
		{Code: "bind_mount", Source: "/srv/skepr-test", Target: "/config", Message: "bind mount /srv/skepr-test -> /config may not be portable across nodes"},
		{Code: "node_local_volume", Source: "skepr-test-data", Target: "/data", Message: "volume skepr-test-data -> /data uses node-local storage"},
	}, diagnosis.PlacementEligibility.StoragePortabilityWarnings)
	require.Len(t, diagnosis.PlacementEligibility.Nodes, 5)
	for _, node := range diagnosis.PlacementEligibility.Nodes {
		wantCodes := []string{"constraint_mismatch", "platform_mismatch", "insufficient_memory"}
		if node.Hostname == "worker-2" {
			wantCodes = append([]string{"node_not_active"}, wantCodes...)
		}
		assert.False(t, node.PassesEvaluatedChecks, node.Hostname)
		assert.Zero(t, node.ActiveServiceTasks, node.Hostname)
		assert.Empty(t, node.UsedHostPorts, node.Hostname)
		require.Len(t, node.Blockers, len(wantCodes), node.Hostname)
		for index, code := range wantCodes {
			assert.Equal(t, code, node.Blockers[index].Code, node.Hostname)
		}
		assert.Contains(t, node.Blockers[len(node.Blockers)-2].Message, "does not match required windows", node.Hostname)
		assert.Positive(t, node.Resources.Capacity.MemoryBytes, node.Hostname)
		assert.LessOrEqual(t, node.Resources.Available.MemoryBytes, node.Resources.Capacity.MemoryBytes, node.Hostname)
	}

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

func drainNode(t *testing.T, connector status.Connector, node string) drain.Result {
	t.Helper()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := cli.Run(context.Background(), []string{"node", "drain", node, "--timeout", "30s", "--json"}, connector, &stdout, &stderr)
	require.Equal(t, cli.ExitSuccess, exitCode, stderr.String())
	var result drain.Result
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
	return result
}

func newLegacyOperation(id string, result drain.Result) maintenance.Operation {
	now := time.Now().UTC()
	return maintenance.Operation{
		SchemaVersion:    maintenance.OperationSchemaVersion,
		ID:               id,
		ClusterID:        result.ClusterID,
		Endpoint:         result.Endpoint,
		Target:           result.Target,
		Phase:            maintenance.PhaseMaintenanceReady,
		PhaseTimestamps:  map[maintenance.Phase]time.Time{maintenance.PhaseMaintenanceReady: now},
		MutationOccurred: true,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}
