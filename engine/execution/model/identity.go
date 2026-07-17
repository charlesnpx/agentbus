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

type PIDNamespaceState uint8

const (
	PIDNamespaceUnknown PIDNamespaceState = iota
	PIDNamespaceKnown
	PIDNamespaceNotApplicable
)

func (state PIDNamespaceState) Validate() error {
	switch state {
	case PIDNamespaceUnknown, PIDNamespaceKnown, PIDNamespaceNotApplicable:
		return nil
	default:
		return invalid("pid_namespace.state", "is unknown")
	}
}

type RetainedDomainState uint8

const (
	RetainedDomainNotApplicable RetainedDomainState = iota
	RetainedDomainKnown
	RetainedDomainUnknown
)

func (state RetainedDomainState) Validate() error {
	switch state {
	case RetainedDomainUnknown, RetainedDomainKnown, RetainedDomainNotApplicable:
		return nil
	default:
		return invalid("retained_domain.state", "is unknown")
	}
}

type KernelDomainID struct {
	HostBootID          string
	PIDNamespaceID      string
	PIDNamespaceState   PIDNamespaceState
	RetainedDomainID    string
	RetainedDomainState RetainedDomainState
}

func NewKernelDomainID(hostBootID, pidNamespaceID string) (KernelDomainID, error) {
	if pidNamespaceID == "" {
		return KernelDomainID{}, invalid("kernel_domain.pid_namespace", "generic constructor requires an id")
	}
	id := KernelDomainID{
		HostBootID:          hostBootID,
		PIDNamespaceID:      pidNamespaceID,
		PIDNamespaceState:   PIDNamespaceKnown,
		RetainedDomainState: RetainedDomainNotApplicable,
	}
	if err := id.Validate(); err != nil {
		return KernelDomainID{}, err
	}
	return id, nil
}

func NewKernelDomainIDWithRetainedDomain(hostBootID, pidNamespaceID, retainedDomainID string) (KernelDomainID, error) {
	if pidNamespaceID == "" {
		return KernelDomainID{}, invalid("kernel_domain.pid_namespace", "retained-domain constructor requires a pid namespace id")
	}
	if retainedDomainID == "" {
		return KernelDomainID{}, invalid("kernel_domain.retained_domain", "retained-domain constructor requires an id")
	}
	id := KernelDomainID{
		HostBootID:          hostBootID,
		PIDNamespaceID:      pidNamespaceID,
		PIDNamespaceState:   PIDNamespaceKnown,
		RetainedDomainID:    retainedDomainID,
		RetainedDomainState: RetainedDomainKnown,
	}
	if err := id.Validate(); err != nil {
		return KernelDomainID{}, err
	}
	return id, nil
}

func NewKernelDomainIDWithoutPIDNamespace(hostBootID string) (KernelDomainID, error) {
	id := KernelDomainID{
		HostBootID:          hostBootID,
		PIDNamespaceState:   PIDNamespaceNotApplicable,
		RetainedDomainState: RetainedDomainNotApplicable,
	}
	if err := id.Validate(); err != nil {
		return KernelDomainID{}, err
	}
	return id, nil
}

func (id KernelDomainID) Validate() error {
	if err := validateToken("kernel_domain.host_boot_id", id.HostBootID); err != nil {
		return err
	}
	if err := validatePIDNamespace("kernel_domain.pid_namespace", id.PIDNamespaceID, id.PIDNamespaceState); err != nil {
		return err
	}
	return validateRetainedDomain("kernel_domain.retained_domain", id.RetainedDomainID, id.RetainedDomainState)
}

func (id KernelDomainID) Equal(other KernelDomainID) bool {
	return id.HostBootID == other.HostBootID &&
		id.PIDNamespaceID == other.PIDNamespaceID &&
		id.PIDNamespaceState == other.PIDNamespaceState &&
		id.RetainedDomainID == other.RetainedDomainID &&
		id.RetainedDomainState == other.RetainedDomainState
}

func (id KernelDomainID) ProvablySame(other KernelDomainID) bool {
	relation, err := compareKernelDomain(id, other)
	if err != nil {
		return false
	}
	return relation == kernelDomainSame
}

type normalizedPIDNamespace struct {
	state PIDNamespaceState
	id    string
}

type normalizedRetainedDomain struct {
	state RetainedDomainState
	id    string
}

type kernelDomainRelation uint8

const (
	kernelDomainSame kernelDomainRelation = iota + 1
	kernelDomainDifferent
	kernelDomainUnprovable
)

func compareKernelDomain(left, right KernelDomainID) (kernelDomainRelation, error) {
	if err := left.Validate(); err != nil {
		return 0, err
	}
	if err := right.Validate(); err != nil {
		return 0, err
	}
	if left.HostBootID != right.HostBootID {
		return kernelDomainDifferent, nil
	}
	leftNamespace := normalizePIDNamespace(left.PIDNamespaceID, left.PIDNamespaceState)
	rightNamespace := normalizePIDNamespace(right.PIDNamespaceID, right.PIDNamespaceState)
	if leftNamespace.state == PIDNamespaceKnown && rightNamespace.state == PIDNamespaceKnown {
		if leftNamespace.id == rightNamespace.id {
			return compareRetainedDomain(left, right), nil
		}
		return kernelDomainDifferent, nil
	}
	if leftNamespace.state == PIDNamespaceNotApplicable && rightNamespace.state == PIDNamespaceNotApplicable {
		return compareRetainedDomain(left, right), nil
	}
	return kernelDomainUnprovable, nil
}

func compareRetainedDomain(left, right KernelDomainID) kernelDomainRelation {
	leftDomain := normalizeRetainedDomain(left.RetainedDomainID, left.RetainedDomainState)
	rightDomain := normalizeRetainedDomain(right.RetainedDomainID, right.RetainedDomainState)
	if leftDomain.state == RetainedDomainKnown && rightDomain.state == RetainedDomainKnown {
		if leftDomain.id == rightDomain.id {
			return kernelDomainSame
		}
		return kernelDomainDifferent
	}
	if leftDomain.state == RetainedDomainNotApplicable && rightDomain.state == RetainedDomainNotApplicable {
		return kernelDomainSame
	}
	return kernelDomainUnprovable
}

func validatePIDNamespace(field string, id string, state PIDNamespaceState) error {
	if err := validateOptionalToken(field+"_id", id); err != nil {
		return err
	}
	if err := state.Validate(); err != nil {
		return err
	}
	if id != "" {
		if state == PIDNamespaceUnknown {
			return invalid(field, "cannot carry an id when state is unknown")
		}
		if state == PIDNamespaceNotApplicable {
			return invalid(field, "cannot carry an id when not applicable")
		}
		return nil
	}
	if state == PIDNamespaceKnown {
		return invalid(field, "known namespace requires an id")
	}
	return nil
}

func normalizePIDNamespace(id string, state PIDNamespaceState) normalizedPIDNamespace {
	if id != "" {
		return normalizedPIDNamespace{state: PIDNamespaceKnown, id: id}
	}
	if state == PIDNamespaceNotApplicable {
		return normalizedPIDNamespace{state: PIDNamespaceNotApplicable}
	}
	return normalizedPIDNamespace{state: PIDNamespaceUnknown}
}

func validateRetainedDomain(field string, id string, state RetainedDomainState) error {
	if err := validateOptionalToken(field+"_id", id); err != nil {
		return err
	}
	if err := state.Validate(); err != nil {
		return err
	}
	if id != "" {
		if state == RetainedDomainUnknown {
			return invalid(field, "cannot carry an id when state is unknown")
		}
		if state == RetainedDomainNotApplicable {
			return invalid(field, "cannot carry an id when not applicable")
		}
		return nil
	}
	if state == RetainedDomainKnown {
		return invalid(field, "known domain requires an id")
	}
	return nil
}

func normalizeRetainedDomain(id string, state RetainedDomainState) normalizedRetainedDomain {
	if id != "" {
		return normalizedRetainedDomain{state: RetainedDomainKnown, id: id}
	}
	if state == RetainedDomainUnknown {
		return normalizedRetainedDomain{state: RetainedDomainUnknown}
	}
	return normalizedRetainedDomain{state: RetainedDomainNotApplicable}
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
