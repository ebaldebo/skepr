package status

import (
	"context"
	"io"
)

const SchemaVersion = 1

type Cluster struct {
	ID               string `json:"id"`
	LocalState       string `json:"local_state"`
	ControlAvailable bool   `json:"control_available"`
}

type Result struct {
	SchemaVersion int     `json:"schema_version"`
	Endpoint      string  `json:"endpoint"`
	Cluster       Cluster `json:"cluster"`
}

type Inspector interface {
	Inspect(context.Context) (Result, error)
}

type Connection interface {
	Inspector
	io.Closer
}

type Connector interface {
	Connect(context.Context, string) (Connection, error)
}
