package dataplane

import (
	"context"

	process "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
)

// PTYSize is the private terminal geometry.
type PTYSize struct{ Cols, Rows uint32 }

// PTYConfig is the private terminal start request.
type PTYConfig struct {
	Command string
	Args    []string
	Env     map[string]string
	CWD     string
	Size    PTYSize
}

// StartPTY waits for the process start barrier before returning the local stream.
func (c *Client) StartPTY(ctx context.Context, config PTYConfig, user string) (*ProcessStream, error) {
	requestConfig := ProcessConfig{Command: config.Command, Args: config.Args, Env: config.Env, CWD: config.CWD, pty: &config.Size}
	stream, err := c.process.Start(ctx, request(c, &process.StartRequest{Process: processConfig(requestConfig), Pty: pty(requestConfig.pty)}, user))
	if err != nil {
		return nil, wrapWireError(err)
	}
	// PTY retains the runtime client's streaming behavior. The independent 2 MiB
	// command-frame guard applies to retained command output, not terminal frames.
	return startedProcess(startReceiver{stream: stream}, 0)
}

// SendPTYInput writes terminal bytes to the generation-bound PID.
func (c *Client) SendPTYInput(ctx context.Context, pid uint32, data []byte, user string) error {
	input := &process.ProcessInput{Input: &process.ProcessInput_Pty{Pty: data}}
	_, err := c.process.SendInput(ctx, request(c, &process.SendInputRequest{Process: selector(pid), Input: input}, user))
	return wrapWireError(err)
}

// ResizePTY updates terminal geometry for the generation-bound PID.
func (c *Client) ResizePTY(ctx context.Context, pid uint32, size PTYSize, user string) error {
	_, err := c.process.Update(ctx, request(c, &process.UpdateRequest{Process: selector(pid), Pty: pty(&size)}, user))
	return wrapWireError(err)
}

func pty(size *PTYSize) *process.PTY {
	if size == nil {
		return nil
	}
	return &process.PTY{Size: &process.PTY_Size{Cols: size.Cols, Rows: size.Rows}}
}
