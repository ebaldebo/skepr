package operations

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorePreventsConcurrentClusterOperations(t *testing.T) {
	stateDir := t.TempDir()
	firstStore := NewStore(stateDir)
	secondStore := NewStore(stateDir)

	firstLock, err := firstStore.AcquireClusterLock("cluster-1")
	require.NoError(t, err)

	_, err = secondStore.AcquireClusterLock("cluster-1")
	var conflict *LockConflictError
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, "cluster-1", conflict.ClusterID)
	assert.Equal(t, "maintenance operation already active for cluster cluster-1", conflict.Error())

	otherClusterLock, err := secondStore.AcquireClusterLock("cluster-2")
	require.NoError(t, err)
	require.NoError(t, otherClusterLock.Release())

	require.NoError(t, firstLock.Release())
	reacquiredLock, err := secondStore.AcquireClusterLock("cluster-1")
	require.NoError(t, err)
	require.NoError(t, reacquiredLock.Release())
}
