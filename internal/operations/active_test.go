package operations

import (
	"testing"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreFindsActiveClusterOperation(t *testing.T) {
	store := NewStore(t.TempDir())
	require.NoError(t, store.Save(Record{
		SchemaVersion: SchemaVersion,
		ID:            "completed-operation",
		ClusterID:     "cluster-1",
		Phase:         maintenance.PhaseCompleted,
	}))
	require.NoError(t, store.Save(Record{
		SchemaVersion: SchemaVersion,
		ID:            "other-cluster-operation",
		ClusterID:     "cluster-2",
		Phase:         maintenance.PhaseEvacuating,
	}))

	active, err := store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	assert.Nil(t, active)
	require.NoError(t, store.EnsureNoActiveOperation("cluster-1"))

	record := Record{
		SchemaVersion: SchemaVersion,
		ID:            "active-operation",
		ClusterID:     "cluster-1",
		Phase:         maintenance.PhaseWaitingServices,
		LastError:     "service convergence timed out",
	}
	require.NoError(t, store.Save(record))

	active, err = store.ActiveForCluster("cluster-1")
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, record, *active)

	err = store.EnsureNoActiveOperation("cluster-1")
	var conflict *ActiveOperationError
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, "cluster-1", conflict.ClusterID)
	assert.Equal(t, "active-operation", conflict.OperationID)
	assert.Equal(t, maintenance.PhaseWaitingServices, conflict.Phase)
	assert.Equal(t, "maintenance operation active-operation is already active for cluster cluster-1 in phase waiting-services", conflict.Error())
}
