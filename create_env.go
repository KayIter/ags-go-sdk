package ags

import (
	"regexp"
	"strings"
)

var createEnvKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func copyCreateEnv(opts CreateOptions) (CreateOptions, error) {
	values := make(map[string]string, len(opts.Env))
	for key, value := range opts.Env {
		if !createEnvKey.MatchString(key) || strings.ContainsRune(value, 0) {
			return opts, codeError(InvalidArgument, "Sandboxes.Create", "ENV_INVALID")
		}
		values[key] = value
	}
	if len(values) > 0 {
		opts.Env = values
	}
	opts.Metadata = cloneStringMap(opts.Metadata)
	opts.MountOptions = append([]MountOption(nil), opts.MountOptions...)
	for i := range opts.MountOptions {
		if opts.MountOptions[i].ReadOnly != nil {
			value := *opts.MountOptions[i].ReadOnly
			opts.MountOptions[i].ReadOnly = &value
		}
	}
	return opts, nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}
