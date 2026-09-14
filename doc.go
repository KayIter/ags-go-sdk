// Package ags provides native Sandbox control, files, processes, PTY, watch,
// pause/resume and Cloud Monitor metrics for Tencent Cloud Agent Runtime.
//
// Construct a Client with explicit region and credential options for services
// that use multiple accounts. The sandbox subpackage exposes environment-backed
// shortcuts returning the same Sandbox type. Control and Metrics use the same
// Tencent Cloud credential provider.
//
// Remote operations accept context.Context. Use deadlines, preserve structured
// errors with errors.As, and never infer that cancellation undid a side effect.
// The SDK does not automatically retry mutations. Create waits for readiness but
// never assumes deletion ownership of an accepted or replayed instance.
//
// Close a Sandbox to release only its local resources; issue Delete explicitly
// for owned remote resources with an independent cleanup context. PTY Close can
// signal the remote process, unlike Sandbox or background CommandHandle Close.
// Close readers and watches, drain bounded event streams, and do not mutate
// input maps, slices or pointed-to options concurrently with an operation.
//
// This pre-release SDK preserves service limitations. Connect can resume
// or extend lifetime; Get is read-only. List supports the current service's
// exact-value AND filters rather than inventing unsupported projections. Create
// Env submission does not establish runtime inheritance or recovery persistence.
package ags
