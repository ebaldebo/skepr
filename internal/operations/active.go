package operations

import (
	"fmt"
	"os"
	"strings"

	"github.com/ebaldebo/skepr/internal/maintenance"
)

type ActiveOperationError struct {
	ClusterID   string
	OperationID string
	Phase       maintenance.Phase
}

func (e *ActiveOperationError) Error() string {
	return fmt.Sprintf("maintenance operation %s is already active for cluster %s in phase %s", e.OperationID, e.ClusterID, e.Phase)
}

func (s *Store) ActiveForCluster(clusterID string) (*Record, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("cluster ID is required")
	}
	entries, err := os.ReadDir(s.operationsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read operation directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		record, err := s.Load(id)
		if err != nil {
			return nil, err
		}
		if record.ClusterID == clusterID && record.Phase != maintenance.PhaseCompleted {
			return &record, nil
		}
	}
	return nil, nil
}

func (s *Store) EnsureNoActiveOperation(clusterID string) error {
	active, err := s.ActiveForCluster(clusterID)
	if err != nil {
		return err
	}
	if active == nil {
		return nil
	}
	return &ActiveOperationError{
		ClusterID:   active.ClusterID,
		OperationID: active.ID,
		Phase:       active.Phase,
	}
}

func (s *Store) LatestActive() (*Record, error) {
	entries, err := os.ReadDir(s.operationsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read operation directory: %w", err)
	}
	var latest *Record
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		record, err := s.Load(id)
		if err != nil {
			return nil, err
		}
		if record.Phase == maintenance.PhaseCompleted {
			continue
		}
		if latest == nil || record.UpdatedAt.After(latest.UpdatedAt) || record.UpdatedAt.Equal(latest.UpdatedAt) && record.ID > latest.ID {
			current := record
			latest = &current
		}
	}
	return latest, nil
}
