package operations

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ebaldebo/skepr/internal/maintenance"
	"golang.org/x/sys/unix"
)

type LockConflictError struct {
	ClusterID string
}

func (e *LockConflictError) Error() string {
	return fmt.Sprintf("maintenance operation already active for cluster %s", e.ClusterID)
}

type ClusterLock struct {
	file     *os.File
	released bool
}

func (s *Store) AcquireClusterLock(clusterID string) (maintenance.ClusterLock, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("cluster ID is required")
	}
	if err := os.MkdirAll(s.locksDir, 0o700); err != nil {
		return nil, fmt.Errorf("create operation lock directory: %w", err)
	}

	lockName := fmt.Sprintf("%x.lock", sha256.Sum256([]byte(clusterID)))
	file, err := os.OpenFile(filepath.Join(s.locksDir, lockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open operation lock for cluster %s: %w", clusterID, err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, &LockConflictError{ClusterID: clusterID}
		}
		return nil, fmt.Errorf("lock cluster %s: %w", clusterID, err)
	}
	return &ClusterLock{file: file}, nil
}

func (l *ClusterLock) Release() error {
	if l == nil || l.released {
		return nil
	}
	l.released = true
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("release cluster lock: %w", err)
	}
	return nil
}
