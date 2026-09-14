package ags

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const codePort = "49999"

type codeTransport interface {
	codeRequest(context.Context, string, string, []byte) (io.ReadCloser, error)
}

func (d *legacyDataPlane) codeRequest(ctx context.Context, operation, requestPath string, body []byte) (out io.ReadCloser, err error) {
	ctx, finish, started := d.requestOperation(ctx, operation)
	defer func() {
		err = operationError(ctx, operation, err)
		if out == nil {
			finish()
		}
	}()
	u, err := url.Parse(d.config.BaseURL)
	if err != nil {
		return nil, err
	}
	hostname := u.Hostname()
	if strings.HasPrefix(hostname, "49983-") {
		hostname = codePort + strings.TrimPrefix(hostname, "49983")
		u.Host = hostname
		if port := u.Port(); port != "" {
			u.Host += ":" + port
		}
	}
	u.Path, u.RawQuery = requestPath, ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Access-Token", d.config.AccessToken)
	response, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = response.Body.Close()
		return nil, dataPlaneHTTPError(operation, response.StatusCode)
	}
	started()
	return &generationReader{ReadCloser: response.Body, ctx: ctx, finish: finish, op: operation}, nil
}
