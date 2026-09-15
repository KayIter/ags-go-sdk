package dataplane

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"

	"connectrpc.com/connect"
	process "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process"
	rpc "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/process/processconnect"
)

// CommandFrameLimit bounds one decoded Connect frame independently from SDK aggregation.
const CommandFrameLimit = 2 << 20

type commandHTTP struct{ client *http.Client }

func (c commandHTTP) Do(req *http.Request) (*http.Response, error) {
	response, err := c.client.Do(req)
	if err == nil && response.StatusCode/100 == 2 {
		response.Body = &commandFrameBody{ReadCloser: response.Body, headerOffset: 5}
	}
	return response, err
}

type commandFrameBody struct {
	io.ReadCloser
	header       [5]byte
	headerOffset int
	remaining    uint32
	failure      error
}

func (b *commandFrameBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.failure != nil {
		return 0, b.failure
	}
	if b.headerOffset == 5 && b.remaining == 0 {
		if _, err := io.ReadFull(b.ReadCloser, b.header[:]); err != nil {
			b.failure = err
			return 0, err
		}
		b.remaining = binary.BigEndian.Uint32(b.header[1:])
		if b.remaining > CommandFrameLimit {
			b.failure = connect.NewError(connect.CodeResourceExhausted, errors.New("COMMAND_FRAME_TOO_LARGE"))
			_ = b.ReadCloser.Close()
			return 0, b.failure
		}
		b.headerOffset = 0
	}
	if b.headerOffset < 5 {
		n := copy(p, b.header[b.headerOffset:])
		b.headerOffset += n
		return n, nil
	}
	if len(p) > int(b.remaining) {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= uint32(n)
	if err == io.EOF && b.remaining > 0 {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

// ProcessConfig is the private wire-independent process request.
type ProcessConfig struct {
	Command string
	Args    []string
	Env     map[string]string
	CWD     string
	pty     *PTYSize
}

// ProcessEventKind classifies normalized process events.
type ProcessEventKind uint8

const (
	ProcessStdout ProcessEventKind = iota + 1
	ProcessStderr
	ProcessPTY
	ProcessEnd
)

// ProcessExit is the normalized terminal status.
type ProcessExit struct {
	Code    int
	Exited  bool
	Status  string
	Message string
}

// ProcessEvent contains one normalized stream event.
type ProcessEvent struct {
	Kind ProcessEventKind
	Data []byte
	Exit *ProcessExit
}

// ProcessInfo is normalized process metadata.
type ProcessInfo struct {
	PID      uint32
	Tag, CWD *string
	Command  string
	Args     []string
	Env      map[string]string
}

// Signal is a private process signal.
type Signal uint8

const (
	SignalTERM Signal = iota + 1
	SignalKILL
)

type processReceiver interface {
	Receive() bool
	Event() *process.ProcessEvent
	Err() error
	Close() error
}

type startReceiver struct {
	stream *connect.ServerStreamForClient[process.StartResponse]
}

func (s startReceiver) Receive() bool                { return s.stream.Receive() }
func (s startReceiver) Event() *process.ProcessEvent { return s.stream.Msg().GetEvent() }
func (s startReceiver) Err() error                   { return s.stream.Err() }
func (s startReceiver) Close() error                 { return s.stream.Close() }

type connectReceiver struct {
	stream *connect.ServerStreamForClient[process.ConnectResponse]
}

func (s connectReceiver) Receive() bool                { return s.stream.Receive() }
func (s connectReceiver) Event() *process.ProcessEvent { return s.stream.Msg().GetEvent() }
func (s connectReceiver) Err() error                   { return s.stream.Err() }
func (s connectReceiver) Close() error                 { return s.stream.Close() }

// ProcessStream is a start-barrier-complete private process stream.
type ProcessStream struct {
	PID      uint32
	receiver processReceiver
}

func (c *Client) processClient() rpc.ProcessClient {
	return rpc.NewProcessClient(commandHTTP{c.httpClient}, c.baseURL, connect.WithProtoJSON(), connect.WithReadMaxBytes(CommandFrameLimit))
}

// StartProcess waits for and validates the start barrier.
func (c *Client) StartProcess(ctx context.Context, config ProcessConfig, user string) (*ProcessStream, error) {
	client := c.processClient()
	stream, err := client.Start(ctx, request(c, &process.StartRequest{Process: processConfig(config), Pty: pty(config.pty)}, user))
	if err != nil {
		return nil, wrapWireError(err)
	}
	return startedProcess(startReceiver{stream: stream}, 0)
}

// ConnectProcess waits for a start barrier matching pid.
func (c *Client) ConnectProcess(ctx context.Context, pid uint32, user string) (*ProcessStream, error) {
	client := c.processClient()
	stream, err := client.Connect(ctx, request(c, &process.ConnectRequest{Process: selector(pid)}, user))
	if err != nil {
		return nil, wrapWireError(err)
	}
	return startedProcess(connectReceiver{stream: stream}, pid)
}

func startedProcess(receiver processReceiver, expectedPID uint32) (*ProcessStream, error) {
	for receiver.Receive() {
		event := receiver.Event()
		if event != nil && event.GetKeepalive() != nil {
			continue
		}
		pid := event.GetStart().GetPid()
		if pid == 0 || expectedPID != 0 && pid != expectedPID {
			_ = receiver.Close()
			reason := "START_EVENT_MISSING"
			if expectedPID != 0 {
				reason = "START_EVENT_MISMATCH"
			}
			return nil, protocolError(reason)
		}
		return &ProcessStream{PID: pid, receiver: receiver}, nil
	}
	err := wrapWireError(receiver.Err())
	_ = receiver.Close()
	if err == nil {
		err = protocolError("START_EVENT_MISSING")
	}
	return nil, err
}

// Recv returns the next non-keepalive normalized event.
func (s *ProcessStream) Recv() (ProcessEvent, error) {
	for s.receiver.Receive() {
		event := s.receiver.Event()
		if event == nil {
			return ProcessEvent{}, protocolError("COMMAND_EVENT_INVALID")
		}
		if event.GetKeepalive() != nil {
			continue
		}
		if data := event.GetData(); data != nil {
			switch value := data.Output.(type) {
			case *process.ProcessEvent_DataEvent_Stdout:
				return ProcessEvent{Kind: ProcessStdout, Data: value.Stdout}, nil
			case *process.ProcessEvent_DataEvent_Stderr:
				return ProcessEvent{Kind: ProcessStderr, Data: value.Stderr}, nil
			case *process.ProcessEvent_DataEvent_Pty:
				return ProcessEvent{Kind: ProcessPTY, Data: value.Pty}, nil
			default:
				return ProcessEvent{}, protocolError("COMMAND_OUTPUT_INVALID")
			}
		}
		if end := event.GetEnd(); end != nil {
			message := ""
			if end.Error != nil {
				message = end.GetError()
			}
			return ProcessEvent{Kind: ProcessEnd, Exit: &ProcessExit{Code: int(end.GetExitCode()), Exited: end.GetExited(), Status: end.GetStatus(), Message: message}}, nil
		}
		return ProcessEvent{}, protocolError("COMMAND_EVENT_INVALID")
	}
	if err := s.receiver.Err(); err != nil {
		return ProcessEvent{}, wrapWireError(err)
	}
	return ProcessEvent{}, io.EOF
}

// Close ends local observation without sending a remote signal.
func (s *ProcessStream) Close() error { return wrapWireError(s.receiver.Close()) }

// Invalidate ends local observation without sending a remote signal.
func (s *ProcessStream) Invalidate() error { return wrapWireError(s.receiver.Close()) }

func (c *Client) ListProcesses(ctx context.Context, user string) ([]ProcessInfo, error) {
	response, err := c.process.List(ctx, request(c, &process.ListRequest{}, user))
	if err != nil {
		return nil, wrapWireError(err)
	}
	if response == nil || response.Msg == nil {
		return nil, protocolError("COMMAND_LIST_RESPONSE_MISSING")
	}
	out := make([]ProcessInfo, 0, len(response.Msg.GetProcesses()))
	for _, item := range response.Msg.GetProcesses() {
		if item == nil || item.GetPid() == 0 || item.GetConfig() == nil {
			return nil, protocolError("COMMAND_INFO_INVALID")
		}
		config := item.GetConfig()
		mapped := ProcessInfo{PID: item.GetPid(), Command: config.GetCmd(), Args: append([]string(nil), config.GetArgs()...), Env: cloneMap(config.GetEnvs())}
		if item.Tag != nil {
			value := item.GetTag()
			mapped.Tag = &value
		}
		if config.Cwd != nil {
			value := config.GetCwd()
			mapped.CWD = &value
		}
		out = append(out, mapped)
	}
	return out, nil
}

func (c *Client) SendProcessInput(ctx context.Context, pid uint32, data []byte, user string) error {
	input := &process.ProcessInput{Input: &process.ProcessInput_Stdin{Stdin: data}}
	_, err := c.process.SendInput(ctx, request(c, &process.SendInputRequest{Process: selector(pid), Input: input}, user))
	return wrapWireError(err)
}

func (c *Client) SendProcessSignal(ctx context.Context, pid uint32, signal Signal, user string) error {
	value := process.Signal_SIGNAL_SIGTERM
	if signal == SignalKILL {
		value = process.Signal_SIGNAL_SIGKILL
	}
	_, err := c.process.SendSignal(ctx, request(c, &process.SendSignalRequest{Process: selector(pid), Signal: value}, user))
	return wrapWireError(err)
}

func processConfig(config ProcessConfig) *process.ProcessConfig {
	out := &process.ProcessConfig{Cmd: config.Command, Args: config.Args, Envs: config.Env}
	if config.CWD != "" {
		out.Cwd = &config.CWD
	}
	return out
}

func selector(pid uint32) *process.ProcessSelector {
	return &process.ProcessSelector{Selector: &process.ProcessSelector_Pid{Pid: pid}}
}

func cloneMap(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
