package operations

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/ebaldebo/skepr/internal/preflight"
	"github.com/ebaldebo/skepr/internal/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreSavesAndLoadsOperationAtomically(t *testing.T) {
	stateDir := t.TempDir()
	store := NewStore(stateDir)
	createdAt := time.Date(2026, time.July, 18, 10, 30, 0, 0, time.UTC)
	record := Record{
		SchemaVersion: SchemaVersion,
		ID:            "operation-1",
		ClusterID:     "cluster-1",
		Endpoint:      "ssh://manager-1",
		Target: status.Node{
			ID: "worker-id", Hostname: "worker-1", Role: "worker", State: "ready", Availability: "active",
		},
		Managers: []status.Node{
			{ID: "manager-id", Hostname: "manager-1", Role: "manager", State: "ready", Availability: "active", ManagerStatus: "leader"},
		},
		TargetWorkload: preflight.TargetWorkload{
			DesiredRunningTaskCount: 1,
			Tasks: []preflight.WorkloadTask{
				{ID: "task-1", Name: "database.1", ServiceID: "service-1", Service: "database", State: "running"},
			},
			AffectedServices: []preflight.AffectedService{
				{ID: "service-1", Name: "database", Mode: "replicated", RunningTasks: 1, DesiredTasks: 1, Singleton: true},
			},
		},
		Phase: maintenance.PhaseCreated,
		PhaseTimestamps: map[maintenance.Phase]time.Time{
			maintenance.PhaseCreated: createdAt,
		},
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}

	require.NoError(t, store.Save(record))

	operationPath := filepath.Join(stateDir, "operations", "operation-1.json")
	info, err := os.Stat(operationPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	loaded, err := store.Load(record.ID)
	require.NoError(t, err)
	assert.Equal(t, record, loaded)

	record.Phase = maintenance.PhasePreflightPassed
	record.PhaseTimestamps[maintenance.PhasePreflightPassed] = createdAt.Add(time.Minute)
	record.UpdatedAt = createdAt.Add(time.Minute)
	require.NoError(t, store.Save(record))

	loaded, err = store.Load(record.ID)
	require.NoError(t, err)
	assert.Equal(t, record, loaded)

	entries, err := os.ReadDir(filepath.Join(stateDir, "operations"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "operation-1.json", entries[0].Name())
}

func TestStoreRejectsSchemaV1ActiveOperation(t *testing.T) {
	stateDir := t.TempDir()
	operationsDir := filepath.Join(stateDir, "operations")
	require.NoError(t, os.MkdirAll(operationsDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(operationsDir, "operation-1.json"), []byte(`{
  "schema_version": 1,
  "id": "operation-1",
  "cluster_id": "cluster-1",
  "phase": "reconciling"
}
`), 0o600))

	_, err := NewStore(stateDir).ActiveForCluster("cluster-1")

	assert.ErrorContains(t, err, "unsupported operation schema version 1, expected 2")
}
