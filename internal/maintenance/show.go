package maintenance

import "github.com/ebaldebo/skepr/internal/status"

type LiveAffectedService struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Mode         string `json:"mode"`
	Singleton    bool   `json:"singleton"`
	Present      bool   `json:"present"`
	RunningTasks uint64 `json:"running_tasks"`
	DesiredTasks uint64 `json:"desired_tasks"`
	Converged    bool   `json:"converged"`
}

type LiveOperationState struct {
	Endpoint                string                `json:"endpoint"`
	ClusterID               string                `json:"cluster_id"`
	ClusterMatchesOperation bool                  `json:"cluster_matches_operation"`
	Target                  *status.Node          `json:"target,omitempty"`
	TargetDesiredTasks      int                   `json:"target_desired_running_tasks"`
	AffectedServices        []LiveAffectedService `json:"affected_services"`
}

type ShowResult struct {
	SchemaVersion int                 `json:"schema_version"`
	Operation     Operation           `json:"operation"`
	Live          *LiveOperationState `json:"live,omitempty"`
	LiveError     string              `json:"live_error,omitempty"`
}

func BuildLiveOperationState(operation Operation, inventory status.Result) LiveOperationState {
	live := LiveOperationState{
		Endpoint:                inventory.Endpoint,
		ClusterID:               inventory.Cluster.ID,
		ClusterMatchesOperation: inventory.Cluster.ID == operation.ClusterID,
		AffectedServices:        make([]LiveAffectedService, 0, len(operation.TargetWorkload.AffectedServices)),
	}
	for _, node := range inventory.Nodes {
		if node.ID == operation.Target.ID {
			target := node
			live.Target = &target
			break
		}
	}
	for _, task := range inventory.DesiredTasks {
		if task.DesiredState == "running" && task.NodeID == operation.Target.ID {
			live.TargetDesiredTasks++
		}
	}
	servicesByID := make(map[string]status.Service, len(inventory.Services))
	for _, service := range inventory.Services {
		servicesByID[service.ID] = service
	}
	for _, saved := range operation.TargetWorkload.AffectedServices {
		current, present := servicesByID[saved.ID]
		live.AffectedServices = append(live.AffectedServices, LiveAffectedService{
			ID:           saved.ID,
			Name:         saved.Name,
			Mode:         saved.Mode,
			Singleton:    saved.Singleton,
			Present:      present,
			RunningTasks: current.RunningTasks,
			DesiredTasks: current.DesiredTasks,
			Converged:    present && current.Converged,
		})
	}
	return live
}
