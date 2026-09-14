package ags

import "time"

// ConnectOptions controls sandbox lifetime, not the HTTP or context deadline.
type ConnectOptions struct {
	// Timeout defaults to five minutes. Connect never intentionally shortens a
	// running sandbox's remaining lifetime. Running updates require at least 300 seconds;
	// paused resume accepts at least 30 seconds. Both allow up to 24 hours.
	Timeout *time.Duration
}

func connectTimeout(options []ConnectOptions) (time.Duration, error) {
	if len(options) > 1 {
		return 0, codeError(InvalidArgument, "Sandboxes.Connect", "SINGLE_OPTIONS_REQUIRED")
	}
	timeout := 5 * time.Minute
	if len(options) == 1 && options[0].Timeout != nil {
		timeout = *options[0].Timeout
	}
	if timeout < 30*time.Second || timeout > 24*time.Hour || timeout%time.Second != 0 {
		return 0, codeError(InvalidArgument, "Sandboxes.Connect", "TIMEOUT_OUT_OF_RANGE")
	}
	return timeout, nil
}

func copyResumeOptions(options ResumeOptions) (ResumeOptions, error) {
	if options.Timeout == nil {
		return options, nil
	}
	timeout := *options.Timeout
	if timeout < 30*time.Second || timeout > 24*time.Hour || timeout%time.Second != 0 {
		return ResumeOptions{}, codeError(InvalidArgument, "Sandbox.Resume", "TIMEOUT_OUT_OF_RANGE")
	}
	options.Timeout = &timeout
	return options, nil
}
