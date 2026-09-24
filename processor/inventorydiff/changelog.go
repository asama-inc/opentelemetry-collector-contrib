// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/inventorydiff"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

// changelogSender sends OTLP logs (change events).
type changelogSender interface {
	Send(ctx context.Context, ld plog.Logs) error
}

type httpChangelogSender struct {
	client   *http.Client
	endpoint string
	headers  map[string]string
}

func newHTTPChangelogSender(cfg ChangelogExport) *httpChangelogSender {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &httpChangelogSender{
		client:   &http.Client{Timeout: timeout},
		endpoint: normalizeLogsEndpoint(cfg.Endpoint),
		headers:  cfg.Headers,
	}
}

func normalizeLogsEndpoint(endpoint string) string {
	endpoint = strings.TrimRight(endpoint, "/")
	if strings.HasSuffix(endpoint, "/v1/logs") {
		return endpoint
	}
	return endpoint + "/v1/logs"
}

func (s *httpChangelogSender) Send(ctx context.Context, ld plog.Logs) error {
	reqProto := plogotlp.NewExportRequestFromLogs(ld)
	body, err := reqProto.MarshalProto()
	if err != nil {
		return fmt.Errorf("marshal otlp logs: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	for k, v := range s.headers {
		httpReq.Header.Set(k, v)
	}
	resp, err := s.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("changelog export status %d", resp.StatusCode)
	}
	return nil
}
