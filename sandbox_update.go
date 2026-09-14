package ags

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/cloudapi"
)

// UpdateOptions changes lifetime and upserts metadata. Empty fields are omitted.
// Metadata updates require a single writer per remote instance.
type UpdateOptions struct {
	// Timeout requests a lifetime of 300..86400 whole seconds. Nil
	// omits lifetime changes.
	Timeout *time.Duration
	// MetadataUpsert adds or replaces exact keys while retaining others. Nil/empty omits metadata
	// changes; empty values are preserved, x- keys are rejected.
	MetadataUpsert map[string]string
}

// UpdateResult acknowledges one mutation; it does not imply a trailing Describe or
// independently verified final state.
type UpdateResult struct {
	// InstanceID identifies the sandbox whose mutation was acknowledged.
	InstanceID string
}

// MutationState describes submission evidence, not permission to retry.
type MutationState string

const (
	// MutationNotSent means evidence establishes that no mutation was submitted.
	MutationNotSent MutationState = "NOT_SENT"
	// MutationUnknown means the remote result is uncertain; do not assume the mutation did not
	// occur.
	MutationUnknown MutationState = "UNKNOWN"
)

// MutationPhase identifies where a mutation failure was observed.
type MutationPhase string

const (
	// MutationValidation identifies input validation or local lifecycle acquisition.
	MutationValidation MutationPhase = "VALIDATION"
	// MutationReadMetadata identifies the pre-mutation Cloud metadata read.
	MutationReadMetadata MutationPhase = "READ_METADATA"
	// MutationSubmit identifies mutation submission or its immediate preparation.
	MutationSubmit MutationPhase = "SUBMIT"
)

// MutationInfo describes evidence about a failed mutation, not retry permission.
type MutationInfo struct {
	// State distinguishes evidence that nothing was sent from an unknown submission outcome.
	State MutationState
	// Phase identifies where the failure was observed.
	Phase MutationPhase
	// InstanceID is the mutation target, not proof that the caller owns cleanup.
	InstanceID string
}

type updateControl interface {
	Update(context.Context, string, UpdateOptions) error
}

// lifecycleMutex preserves existing local serialization while allowing Update
// to abandon a lock wait without leaving an orphaned lock-acquisition goroutine.
type lifecycleMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *lifecycleMutex) acquire(ctx context.Context) error {
	m.once.Do(func() { m.token = make(chan struct{}, 1); m.token <- struct{}{} })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.token:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	}
}
func (m *lifecycleMutex) Lock()   { _ = m.acquire(context.Background()) }
func (m *lifecycleMutex) Unlock() { m.token <- struct{}{} }

func updateFailure(id string, state MutationState, phase MutationPhase, err error) error {
	if err == nil {
		return nil
	}
	normalized := normalizeError("Sandbox.Update", err)
	var sdk *Error
	if errors.As(normalized, &sdk) {
		copied := *sdk
		copied.Operation = "Sandbox.Update"
		copied.InstanceID = id
		copied.Mutation = &MutationInfo{State: state, Phase: phase, InstanceID: id}
		return &copied
	}
	return &Error{Code: Unavailable, Operation: "Sandbox.Update", Cause: err, InstanceID: id, Mutation: &MutationInfo{State: state, Phase: phase, InstanceID: id}}
}

func copyUpdate(opts UpdateOptions) (UpdateOptions, error) {
	if opts.Timeout == nil && len(opts.MetadataUpsert) == 0 {
		return UpdateOptions{}, codeError(InvalidArgument, "Sandbox.Update", "UPDATE_REQUIRED")
	}
	copy := UpdateOptions{}
	if opts.Timeout != nil {
		v := *opts.Timeout
		if v < 5*time.Minute || v > 24*time.Hour || v%time.Second != 0 {
			return copy, codeError(InvalidArgument, "Sandbox.Update", "TIMEOUT_OUT_OF_RANGE")
		}
		copy.Timeout = &v
	}
	if len(opts.MetadataUpsert) > 0 {
		copy.MetadataUpsert = make(map[string]string, len(opts.MetadataUpsert))
		for k, v := range opts.MetadataUpsert {
			if k == "" || strings.ContainsAny(k, "\x00\r\n") || strings.ContainsRune(v, 0) {
				return copy, codeError(InvalidArgument, "Sandbox.Update", "METADATA_INVALID")
			}
			if strings.HasPrefix(k, "x-") {
				return copy, codeError(Unsupported, "Sandbox.Update", "RESERVED_METADATA_UNSUPPORTED")
			}
			copy.MetadataUpsert[k] = v
		}
	}
	return copy, nil
}

// Update confirms an update without an implicit Info call. A failed submission
// can have an unknown outcome; it is never retried or rolled back automatically.
func (s *Sandbox) Update(ctx context.Context, opts UpdateOptions) (UpdateResult, error) {
	copy, err := copyUpdate(opts)
	fail := func(e error) (UpdateResult, error) {
		return UpdateResult{}, updateFailure(s.id, MutationNotSent, MutationValidation, e)
	}
	if err != nil {
		return fail(err)
	}
	port, ok := s.client.cfg.control.(updateControl)
	if !ok {
		return fail(codeError(Unsupported, "Sandbox.Update", "UPDATE_UNAVAILABLE"))
	}
	if err = s.lifecycle.acquire(ctx); err != nil {
		return fail(err)
	}
	defer s.lifecycle.Unlock()
	if s.isClosed() {
		return fail(codeError(Conflict, "Sandbox.Update", "SANDBOX_CLOSED"))
	}
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	if err = port.Update(ctx, s.id, copy); err != nil {
		return UpdateResult{}, err
	}
	return UpdateResult{InstanceID: s.id}, nil
}

func (c *tencentControlPlane) Update(ctx context.Context, id string, opts UpdateOptions) error {
	in := cloudapi.UpdateInput{InstanceID: id}
	if opts.Timeout != nil {
		in.Timeout = opts.Timeout.String()
	}
	if len(opts.MetadataUpsert) > 0 {
		metadata, err := c.api.MetadataForUpdate(ctx, id)
		if err != nil {
			return updateFailure(id, MutationNotSent, MutationReadMetadata, mapCloudError(err, "DescribeSandboxInstanceList"))
		}
		for k, v := range opts.MetadataUpsert {
			metadata[k] = v
		}
		in.Metadata = metadata
	}
	if err := ctx.Err(); err != nil {
		return updateFailure(id, MutationNotSent, MutationSubmit, err)
	}
	return updateFailure(id, MutationUnknown, MutationSubmit, mapCloudError(c.api.Update(ctx, in), "UpdateSandboxInstance"))
}
