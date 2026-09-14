package ags

import (
	"strings"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/cloudapi"
)

func copyListOptions(opts SandboxListOptions) SandboxListOptions {
	opts.States = append([]SandboxState(nil), opts.States...)
	metadata := make(map[string][]string, len(opts.Metadata))
	for key, values := range opts.Metadata {
		metadata[key] = append([]string(nil), values...)
	}
	opts.Metadata = metadata
	return opts
}

func cloudListFilters(opts SandboxListOptions) (cloudapi.ListInput, error) {
	in := cloudapi.ListInput{ToolID: opts.ToolID, Offset: opts.Offset, Limit: opts.Limit, Metadata: opts.Metadata}
	for _, state := range opts.States {
		var wire string
		switch state {
		case Creating:
			wire = "STARTING"
		case Running, Pausing, Paused, Stopping, Stopped, Failed:
			wire = string(state)
		case Resuming:
			return cloudapi.ListInput{}, codeError(Unsupported, "Sandboxes.List", "LIST_STATE_UNAVAILABLE")
		default:
			return cloudapi.ListInput{}, codeError(InvalidArgument, "Sandboxes.List", "LIST_STATE_INVALID")
		}
		in.Statuses = append(in.Statuses, wire)
	}
	if len(opts.Metadata) > 5 {
		return cloudapi.ListInput{}, codeError(InvalidArgument, "Sandboxes.List", "LIST_METADATA_INVALID")
	}
	for key, values := range opts.Metadata {
		if key == "" || strings.TrimSpace(key) != key || strings.ContainsRune(key, 0) || len(values) == 0 {
			return cloudapi.ListInput{}, codeError(InvalidArgument, "Sandboxes.List", "LIST_METADATA_INVALID")
		}
	}
	for _, values := range opts.Metadata {
		if len(values) > 1 {
			return cloudapi.ListInput{}, codeError(Unsupported, "Sandboxes.List", "LIST_METADATA_OR_UNAVAILABLE")
		}
	}
	return in, nil
}
