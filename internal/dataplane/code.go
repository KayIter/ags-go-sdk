package dataplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const codePort = "49999"

// CodeContext contains normalized context metadata.
type CodeContext struct{ ID, Language, CWD string }

// CodeRequest is the private execution request.
type CodeRequest struct {
	Source, ContextID, Language string
	Env                         map[string]string
}

// CodeEventKind classifies one normalized Code event.
type CodeEventKind uint8

const (
	CodeKeepalive CodeEventKind = iota + 1
	CodeEnd
	CodeStdout
	CodeStderr
	CodeResultEvent
	CodeErrorEvent
	CodeExecutionCount
)

// CodeResult contains normalized rich result representations.
type CodeResult struct {
	Text, HTML, Markdown, SVG, PNG, JPEG, PDF, Latex, JavaScript *string
	JSON, Data, Chart, Extra                                     map[string]any
	IsMainResult                                                 bool
}

// CodeExecutionError is normalized remote execution data.
type CodeExecutionError struct{ Name, Value, Traceback string }

// CodeEvent is one validated Code wire event.
type CodeEvent struct {
	Kind           CodeEventKind
	Text           string
	Result         CodeResult
	ExecutionError *CodeExecutionError
	ExecutionCount *int
	WireBytes      int
}

// CodeStream owns one newline-delimited Code response.
type CodeStream struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
	ended   bool
}

type createContextRequest struct {
	Language string `json:"language,omitempty"`
	CWD      string `json:"cwd,omitempty"`
}

type createContextResponse struct {
	ID       string `json:"id"`
	Language string `json:"language"`
	CWD      string `json:"cwd"`
}

type runRequest struct {
	Code      string            `json:"code"`
	ContextID string            `json:"context_id,omitempty"`
	Language  string            `json:"language,omitempty"`
	Env       map[string]string `json:"env_vars,omitempty"`
}

type wireCodeResult struct {
	Text         *string        `json:"text,omitempty"`
	HTML         *string        `json:"html,omitempty"`
	Markdown     *string        `json:"markdown,omitempty"`
	SVG          *string        `json:"svg,omitempty"`
	PNG          *string        `json:"png,omitempty"`
	JPEG         *string        `json:"jpeg,omitempty"`
	PDF          *string        `json:"pdf,omitempty"`
	Latex        *string        `json:"latex,omitempty"`
	JavaScript   *string        `json:"javascript,omitempty"`
	JSON         map[string]any `json:"json,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
	Chart        map[string]any `json:"chart,omitempty"`
	Extra        map[string]any `json:"extra,omitempty"`
	IsMainResult bool           `json:"is_main_result"`
}

// openCode sends one Code service request and returns its streaming body.
func (c *Client) openCode(ctx context.Context, requestPath string, body []byte) (io.ReadCloser, error) {
	endpoint, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}
	hostname := endpoint.Hostname()
	if strings.HasPrefix(hostname, "49983-") {
		port := endpoint.Port()
		hostname = codePort + strings.TrimPrefix(hostname, "49983")
		endpoint.Host = hostname
		if port != "" {
			endpoint.Host = net.JoinHostPort(hostname, port)
		}
	}
	endpoint.Path, endpoint.RawQuery = requestPath, ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Access-Token", c.accessToken)
	response, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_ = response.Body.Close()
		return nil, httpError(response.StatusCode)
	}
	return response.Body, nil
}

// CreateCodeContext performs and validates one context creation response.
func (c *Client) CreateCodeContext(ctx context.Context, language, cwd string, maxResponseBytes int64) (CodeContext, error) {
	payload, err := json.Marshal(createContextRequest{Language: language, CWD: cwd})
	if err != nil {
		return CodeContext{}, err
	}
	body, err := c.openCode(ctx, "/contexts", payload)
	if err != nil {
		return CodeContext{}, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return CodeContext{}, err
	}
	if int64(len(data)) > maxResponseBytes {
		return CodeContext{}, &WireError{Kind: ErrorRPC, Code: "RESOURCE_EXHAUSTED", Reason: "CODE_CONTEXT_RESPONSE_TOO_LARGE"}
	}
	var response createContextResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return CodeContext{}, protocolError("CODE_CONTEXT_RESPONSE_INVALID")
	}
	if response.ID == "" {
		return CodeContext{}, protocolError("CODE_CONTEXT_ID_MISSING")
	}
	return CodeContext{ID: response.ID, Language: response.Language, CWD: response.CWD}, nil
}

// StartCode marshals one execution request and returns a validating event stream.
func (c *Client) StartCode(ctx context.Context, request CodeRequest, maxEventBytes int) (*CodeStream, error) {
	payload, err := json.Marshal(runRequest{Code: request.Source, ContextID: request.ContextID, Language: request.Language, Env: request.Env})
	if err != nil {
		return nil, err
	}
	body, err := c.openCode(ctx, "/execute", payload)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), maxEventBytes)
	return &CodeStream{body: body, scanner: scanner}, nil
}

// Recv returns one validated event and rejects unknown or post-end frames.
func (s *CodeStream) Recv() (CodeEvent, error) {
	if !s.scanner.Scan() {
		if err := s.scanner.Err(); err != nil {
			if errors.Is(err, bufio.ErrTooLong) || strings.Contains(err.Error(), "token too long") {
				return CodeEvent{}, &WireError{Kind: ErrorRPC, Code: "RESOURCE_EXHAUSTED", Reason: "CODE_EVENT_TOO_LARGE"}
			}
			return CodeEvent{}, err
		}
		return CodeEvent{}, io.EOF
	}
	line := append([]byte(nil), s.scanner.Bytes()...)
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return CodeEvent{}, protocolError("CODE_EVENT_INVALID")
	}
	if envelope.Type == "" {
		return CodeEvent{}, protocolError("CODE_EVENT_TYPE_MISSING")
	}
	if s.ended {
		return CodeEvent{}, protocolError("CODE_EVENT_AFTER_END")
	}
	event := CodeEvent{WireBytes: len(line)}
	switch envelope.Type {
	case "keepalive":
		event.Kind = CodeKeepalive
	case "end_of_execution":
		event.Kind, s.ended = CodeEnd, true
	case "stdout", "stderr":
		var value struct {
			Text *string `json:"text"`
		}
		if err := json.Unmarshal(line, &value); err != nil || value.Text == nil {
			return CodeEvent{}, protocolError("CODE_LOG_EVENT_INVALID")
		}
		if envelope.Type == "stdout" {
			event.Kind = CodeStdout
		} else {
			event.Kind = CodeStderr
		}
		event.Text = *value.Text
	case "result":
		var value wireCodeResult
		if err := json.Unmarshal(line, &value); err != nil {
			return CodeEvent{}, protocolError("CODE_RESULT_INVALID")
		}
		event.Kind = CodeResultEvent
		event.Result = CodeResult{Text: value.Text, HTML: value.HTML, Markdown: value.Markdown, SVG: value.SVG, PNG: value.PNG, JPEG: value.JPEG, PDF: value.PDF, Latex: value.Latex, JavaScript: value.JavaScript, JSON: value.JSON, Data: value.Data, Chart: value.Chart, Extra: value.Extra, IsMainResult: value.IsMainResult}
	case "error":
		var value struct {
			Name, Value, Traceback string
		}
		if err := json.Unmarshal(line, &value); err != nil {
			return CodeEvent{}, protocolError("CODE_ERROR_EVENT_INVALID")
		}
		event.Kind = CodeErrorEvent
		event.ExecutionError = &CodeExecutionError{Name: value.Name, Value: value.Value, Traceback: value.Traceback}
	case "number_of_executions":
		var value struct {
			ExecutionCount *int `json:"execution_count"`
		}
		if err := json.Unmarshal(line, &value); err != nil || value.ExecutionCount == nil {
			return CodeEvent{}, protocolError("CODE_EXECUTION_COUNT_INVALID")
		}
		event.Kind, event.ExecutionCount = CodeExecutionCount, value.ExecutionCount
	default:
		return CodeEvent{}, protocolErrorDetail("CODE_EVENT_UNKNOWN", fmt.Sprintf("unknown Code event type %q", envelope.Type))
	}
	return event, nil
}

// Close releases the local response body.
func (s *CodeStream) Close() error { return s.body.Close() }

// Invalidate releases the local response body without a remote mutation.
func (s *CodeStream) Invalidate() error { return s.body.Close() }
