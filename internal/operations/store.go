package operations

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ebaldebo/skepr/internal/maintenance"
)

const SchemaVersion = maintenance.OperationSchemaVersion

type Record = maintenance.Operation

type Store struct {
	operationsDir string
	locksDir      string
}

func NewStore(stateDir string) *Store {
	return &Store{
		operationsDir: filepath.Join(stateDir, "operations"),
		locksDir:      filepath.Join(stateDir, "locks"),
	}
}

func DefaultStateDir() (string, error) {
	if stateHome := os.Getenv("XDG_STATE_HOME"); stateHome != "" {
		return filepath.Join(stateHome, "skepr"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory for Skepr state: %w", err)
	}
	return filepath.Join(home, ".local", "state", "skepr"), nil
}

func (s *Store) Save(record Record) error {
	if err := validateOperationID(record.ID); err != nil {
		return err
	}
	if record.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported operation schema version %d", record.SchemaVersion)
	}
	if err := os.MkdirAll(s.operationsDir, 0o700); err != nil {
		return fmt.Errorf("create operation directory: %w", err)
	}

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("encode operation %q: %w", record.ID, err)
	}
	data = append(data, '\n')

	temporary, err := os.CreateTemp(s.operationsDir, ".operation-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary operation %q: %w", record.ID, err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set temporary operation permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write operation %q: %w", record.ID, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync operation %q: %w", record.ID, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close operation %q: %w", record.ID, err)
	}

	operationPath := filepath.Join(s.operationsDir, record.ID+".json")
	if err := os.Rename(temporaryPath, operationPath); err != nil {
		return fmt.Errorf("replace operation %q: %w", record.ID, err)
	}
	removeTemporary = false

	directory, err := os.Open(s.operationsDir)
	if err != nil {
		return fmt.Errorf("open operation directory: %w", err)
	}
	defer func() { _ = directory.Close() }()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync operation directory: %w", err)
	}
	return nil
}

func (s *Store) Load(id string) (Record, error) {
	if err := validateOperationID(id); err != nil {
		return Record{}, err
	}
	data, err := os.ReadFile(filepath.Join(s.operationsDir, id+".json"))
	if err != nil {
		return Record{}, fmt.Errorf("read operation %q: %w", id, err)
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return Record{}, fmt.Errorf("decode operation %q: %w", id, err)
	}
	if record.SchemaVersion != SchemaVersion {
		return Record{}, fmt.Errorf("unsupported operation schema version %d", record.SchemaVersion)
	}
	if record.ID != id {
		return Record{}, fmt.Errorf("operation file %q contains ID %q", id, record.ID)
	}
	return record, nil
}

func validateOperationID(id string) error {
	if id == "" {
		return fmt.Errorf("operation ID is required")
	}
	for _, character := range id {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' {
			continue
		}
		return fmt.Errorf("invalid operation ID %q", id)
	}
	return nil
}
