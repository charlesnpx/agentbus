package model

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type JobID string

func NewJobID(value string) (JobID, error) {
	id := JobID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id JobID) String() string {
	return string(id)
}

func (id JobID) Validate() error {
	return validateToken("job_id", string(id))
}

type WorkspaceKey string

func NewWorkspaceKey(value string) (WorkspaceKey, error) {
	key := WorkspaceKey(value)
	if err := key.Validate(); err != nil {
		return "", err
	}
	return key, nil
}

func (key WorkspaceKey) String() string {
	return string(key)
}

func (key WorkspaceKey) Validate() error {
	return validateToken("workspace_key", string(key))
}

type RequestID string

func NewRequestID(value string) (RequestID, error) {
	id := RequestID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id RequestID) String() string {
	return string(id)
}

func (id RequestID) Validate() error {
	return validateToken("request_id", string(id))
}

type RequestKey struct {
	WorkspaceKey WorkspaceKey
	RequestID    RequestID
}

func NewRequestKey(workspaceKey, requestID string) (RequestKey, error) {
	key := RequestKey{WorkspaceKey: WorkspaceKey(workspaceKey), RequestID: RequestID(requestID)}
	if err := key.Validate(); err != nil {
		return RequestKey{}, err
	}
	return key, nil
}

func (key RequestKey) Validate() error {
	if err := key.WorkspaceKey.Validate(); err != nil {
		return err
	}
	return key.RequestID.Validate()
}

func (key RequestKey) String() string {
	return key.WorkspaceKey.String() + "/" + key.RequestID.String()
}

type Binding struct {
	RequestKey   RequestKey
	JobID        JobID
	TaskIdentity TaskIdentity
	Mode         Mode
}

func (binding Binding) Validate() error {
	if err := binding.RequestKey.Validate(); err != nil {
		return err
	}
	if err := binding.JobID.Validate(); err != nil {
		return err
	}
	if err := binding.TaskIdentity.Validate(); err != nil {
		return err
	}
	return binding.Mode.Validate()
}

func (binding Binding) Matches(record SafetyRecord) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if binding.RequestKey != record.RequestKey {
		return invalid("binding.request_key", "does not match safety record")
	}
	if binding.JobID != record.JobID {
		return invalid("binding.job_id", "does not match safety record")
	}
	if !binding.TaskIdentity.Equal(record.TaskIdentity) {
		return invalid("binding.task_identity", "does not match safety record")
	}
	if binding.Mode != record.Mode {
		return invalid("binding.mode", "does not match safety record")
	}
	return nil
}

type BootID string

func NewBootID(value string) (BootID, error) {
	id := BootID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id BootID) String() string {
	return string(id)
}

func (id BootID) Validate() error {
	return validateToken("boot_id", string(id))
}

type OwnerID string

func NewOwnerID(value string) (OwnerID, error) {
	id := OwnerID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id OwnerID) String() string {
	return string(id)
}

func (id OwnerID) Validate() error {
	return validateToken("owner_id", string(id))
}

type BootRef struct {
	BootID  BootID
	OwnerID OwnerID
}

func NewBootRef(bootID, ownerID string) (BootRef, error) {
	ref := BootRef{BootID: BootID(bootID), OwnerID: OwnerID(ownerID)}
	if err := ref.Validate(); err != nil {
		return BootRef{}, err
	}
	return ref, nil
}

func (ref BootRef) Validate() error {
	if err := ref.BootID.Validate(); err != nil {
		return err
	}
	return ref.OwnerID.Validate()
}

type AttemptID string

func NewAttemptID(value string) (AttemptID, error) {
	id := AttemptID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id AttemptID) String() string {
	return string(id)
}

func (id AttemptID) Validate() error {
	return validateToken("attempt_id", string(id))
}

type AttemptRef struct {
	JobID     JobID
	AttemptID AttemptID
	Epoch     uint64
}

func NewAttemptRef(jobID, attemptID string, epoch uint64) (AttemptRef, error) {
	ref := AttemptRef{JobID: JobID(jobID), AttemptID: AttemptID(attemptID), Epoch: epoch}
	if err := ref.Validate(); err != nil {
		return AttemptRef{}, err
	}
	return ref, nil
}

func (ref AttemptRef) Validate() error {
	if err := ref.JobID.Validate(); err != nil {
		return err
	}
	if err := ref.AttemptID.Validate(); err != nil {
		return err
	}
	return validatePositiveUint64("attempt_epoch", ref.Epoch)
}

func (ref AttemptRef) Equal(other AttemptRef) bool {
	return ref.JobID == other.JobID && ref.AttemptID == other.AttemptID && ref.Epoch == other.Epoch
}

type TaskIdentity struct {
	Algorithm string
	Version   uint16
	Value     string
}

const (
	TaskIdentityAlgorithmSHA256 = "sha256"
	CurrentTaskIdentityVersion  = uint16(1)
)

func NewTaskIdentity(algorithm string, version uint16, value string) (TaskIdentity, error) {
	identity := TaskIdentity{Algorithm: algorithm, Version: version, Value: value}
	if err := identity.Validate(); err != nil {
		return TaskIdentity{}, err
	}
	return identity, nil
}

func NewSHA256TaskIdentity(canonical []byte) TaskIdentity {
	sum := sha256.Sum256(canonical)
	return TaskIdentity{
		Algorithm: TaskIdentityAlgorithmSHA256,
		Version:   CurrentTaskIdentityVersion,
		Value:     hex.EncodeToString(sum[:]),
	}
}

func (identity TaskIdentity) Validate() error {
	if err := validateToken("task_identity.algorithm", identity.Algorithm); err != nil {
		return err
	}
	if err := validatePositiveUint16("task_identity.version", identity.Version); err != nil {
		return err
	}
	if identity.Algorithm != TaskIdentityAlgorithmSHA256 {
		return invalid("task_identity.algorithm", "is unsupported")
	}
	if identity.Version != CurrentTaskIdentityVersion {
		return invalid("task_identity.version", "is unsupported")
	}
	if len(identity.Value) != sha256.Size*2 {
		return invalid("task_identity.value", "must be a sha256 hex digest")
	}
	if identity.Value != strings.ToLower(identity.Value) {
		return invalid("task_identity.value", "must be lowercase")
	}
	if _, err := hex.DecodeString(identity.Value); err != nil {
		return invalid("task_identity.value", "must be hexadecimal")
	}
	return nil
}

func (identity TaskIdentity) Equal(other TaskIdentity) bool {
	return identity.Algorithm == other.Algorithm && identity.Version == other.Version && identity.Value == other.Value
}
