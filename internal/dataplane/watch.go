package dataplane

import (
	"context"
	"io"
	"path"

	"connectrpc.com/connect"
	fs "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/gen/filesystem"
)

// WatchEventType is the private normalized filesystem event kind.
type WatchEventType uint8

const (
	WatchUnknown WatchEventType = iota
	WatchCreate
	WatchWrite
	WatchRemove
	WatchRename
	WatchChmod
)

// WatchEvent is a normalized wire event.
type WatchEvent struct {
	Name  string
	Type  WatchEventType
	Entry *FileInfo
}

// WatchStream is a start-barrier-complete filesystem stream.
type WatchStream struct {
	stream       *connect.ServerStreamForClient[fs.WatchDirResponse]
	client       *Client
	root         string
	user         string
	includeEntry bool
}

// StartWatch waits for the runtime start barrier before returning.
func (c *Client) StartWatch(ctx context.Context, path string, recursive, includeEntry bool, user string) (*WatchStream, error) {
	stream, err := c.filesystem.WatchDir(ctx, request(c, &fs.WatchDirRequest{Path: path, Recursive: recursive}, user))
	if err != nil {
		return nil, wrapWireError(err)
	}
	for stream.Receive() {
		message := stream.Msg()
		if message != nil && message.GetStart() != nil {
			return &WatchStream{stream: stream, client: c, root: path, user: user, includeEntry: includeEntry}, nil
		}
	}
	err = wrapWireError(stream.Err())
	_ = stream.Close()
	if err == nil {
		err = protocolError("START_EVENT_MISSING")
	}
	return nil, err
}

// Recv returns the next normalized filesystem event.
func (s *WatchStream) Recv(ctx context.Context) (WatchEvent, error) {
	for s.stream.Receive() {
		message := s.stream.Msg()
		if message == nil || message.GetKeepalive() != nil || message.GetStart() != nil {
			continue
		}
		event := message.GetFilesystem()
		if event == nil {
			continue
		}
		out := WatchEvent{Name: event.GetName(), Type: watchEventType(event.GetType())}
		if s.includeEntry && out.Type != WatchRemove {
			entryPath := out.Name
			if !path.IsAbs(entryPath) {
				entryPath = path.Join(s.root, entryPath)
			}
			if info, err := s.client.StatFile(ctx, entryPath, s.user); err == nil {
				out.Entry = &info
			}
		}
		return out, nil
	}
	if err := s.stream.Err(); err != nil {
		return WatchEvent{}, wrapWireError(err)
	}
	return WatchEvent{}, io.EOF
}

// Close ends normal local observation.
func (s *WatchStream) Close() error { return wrapWireError(s.stream.Close()) }

// Invalidate ends local observation without any remote mutation.
func (s *WatchStream) Invalidate() error { return wrapWireError(s.stream.Close()) }

func watchEventType(value fs.EventType) WatchEventType {
	switch value {
	case fs.EventType_EVENT_TYPE_CREATE:
		return WatchCreate
	case fs.EventType_EVENT_TYPE_WRITE:
		return WatchWrite
	case fs.EventType_EVENT_TYPE_REMOVE:
		return WatchRemove
	case fs.EventType_EVENT_TYPE_RENAME:
		return WatchRename
	case fs.EventType_EVENT_TYPE_CHMOD:
		return WatchChmod
	default:
		return WatchUnknown
	}
}
