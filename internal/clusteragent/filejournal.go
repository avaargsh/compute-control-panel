package clusteragent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type journalRecord struct {
	CommandID     string          `json:"command_id"`
	RequestDigest string          `json:"request_digest"`
	Action        CommandAction   `json:"action"`
	Fence         uint64          `json:"fence"`
	State         string          `json:"state"`
	Result        ExecutionResult `json:"result"`
}

// FileJournal is a single-writer append-only execution journal. Mount its file
// on persistent storage and run one active process per Agent identity. The
// control-plane command lease still fences remote completions; this journal
// prevents a local process restart from replaying an uncertain APPLY.
type FileJournal struct {
	mu      sync.Mutex
	file    *os.File
	records map[string]journalRecord
}

func OpenFileJournal(path string) (*FileJournal, error) {
	if path == "" || filepath.Clean(path) != path {
		return nil, ErrInvalidConfiguration
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create Cluster Agent journal directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open Cluster Agent journal: %w", err)
	}
	journal := &FileJournal{file: file, records: make(map[string]journalRecord)}
	if err := journal.load(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return journal, nil
}

func (j *FileJournal) load() error {
	if j == nil || j.file == nil {
		return ErrInvalidConfiguration
	}
	if _, err := j.file.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(j.file)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		var record journalRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return fmt.Errorf("read Cluster Agent journal record: %w", err)
		}
		if record.CommandID == "" || record.RequestDigest == "" ||
			(record.State != "EXECUTING" && record.State != "COMPLETED") {
			return errors.New("Cluster Agent journal contains an invalid record")
		}
		j.records[record.CommandID] = record
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan Cluster Agent journal: %w", err)
	}
	_, err := j.file.Seek(0, 2)
	return err
}

func (j *FileJournal) append(record journalRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := j.file.Write(append(encoded, '\n')); err != nil {
		return err
	}
	if err := j.file.Sync(); err != nil {
		return err
	}
	j.records[record.CommandID] = record
	return nil
}

func (j *FileJournal) Close() error {
	if j == nil || j.file == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.file.Close()
	j.file = nil
	return err
}

func commandFingerprint(command Command) string {
	fingerprint := struct {
		CommandID     string        `json:"command_id"`
		Action        CommandAction `json:"action"`
		AgentID       string        `json:"agent_id"`
		ProviderID    string        `json:"provider_id"`
		OperationID   string        `json:"operation_id"`
		RequestDigest string        `json:"request_digest"`
	}{command.CommandID, command.Action, command.AgentID, command.ProviderID, command.OperationID, command.RequestDigest}
	encoded, _ := json.Marshal(fingerprint)
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Execute records EXECUTING and fsyncs it before invoking the external Provider.
// A completed command is replayed from disk. An interrupted APPLY returns
// UNKNOWN and is never executed a second time. A read-only INSPECT may be
// retried only after the control plane issues a higher command fence.
func (j *FileJournal) Execute(ctx context.Context, command Command, executor Executor) ExecutionResult {
	unknown := ExecutionResult{Outcome: OutcomeUnknown, ErrorCode: "LOCAL_JOURNAL_UNAVAILABLE"}
	if j == nil || executor == nil || command.Validate() != nil {
		return ExecutionResult{Outcome: OutcomeFailed, ErrorCode: "INVALID_COMMAND"}
	}
	fingerprint := commandFingerprint(command)

	j.mu.Lock()
	if j.file == nil {
		j.mu.Unlock()
		return unknown
	}
	previous, exists := j.records[command.CommandID]
	if exists {
		if previous.RequestDigest != fingerprint || previous.Action != command.Action {
			j.mu.Unlock()
			return ExecutionResult{Outcome: OutcomeFailed, ErrorCode: "COMMAND_IDENTITY_CONFLICT"}
		}
		if previous.State == "COMPLETED" {
			j.mu.Unlock()
			return previous.Result
		}
		if command.Action == ActionApply || command.CommandFence <= previous.Fence {
			j.mu.Unlock()
			return ExecutionResult{Outcome: OutcomeUnknown, ErrorCode: "PREVIOUS_EXECUTION_UNCERTAIN"}
		}
	}
	if err := j.append(journalRecord{
		CommandID: command.CommandID, RequestDigest: fingerprint,
		Action: command.Action, Fence: command.CommandFence, State: "EXECUTING",
	}); err != nil {
		j.mu.Unlock()
		return unknown
	}
	j.mu.Unlock()

	result := executor.Execute(ctx, command)
	if err := result.Validate(); err != nil {
		result = ExecutionResult{Outcome: OutcomeUnknown, ErrorCode: "INVALID_EXECUTOR_RESULT"}
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return unknown
	}
	if err := j.append(journalRecord{
		CommandID: command.CommandID, RequestDigest: fingerprint,
		Action: command.Action, Fence: command.CommandFence,
		State: "COMPLETED", Result: result,
	}); err != nil {
		return unknown
	}
	return result
}

var _ LocalJournal = (*FileJournal)(nil)

var _ = context.Canceled
