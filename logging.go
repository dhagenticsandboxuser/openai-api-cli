package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// traceLogger writes ordered JSON lines. Logging is disabled with a nil writer.
// The credential itself is never logged; known authentication headers are masked.
type traceLogger struct {
	mu       sync.Mutex
	writer   io.Writer
	key      string
	sequence int
	err      error
}

// record timestamps each step and remembers logging failures for the CLI to report.
func (l *traceLogger) record(step string, fields map[string]any) {
	if l == nil || l.writer == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return
	}
	l.sequence++
	if fields == nil {
		fields = make(map[string]any)
	}
	fields["step"], fields["sequence"], fields["time"] = step, l.sequence, time.Now().UTC().Format(time.RFC3339Nano)
	b, err := json.Marshal(fields)
	if err == nil && l.key != "" {
		secret, _ := json.Marshal(l.key)
		b = bytes.ReplaceAll(b, secret[1:len(secret)-1], []byte("[REDACTED]"))
	}
	if err == nil {
		_, err = l.writer.Write(append(b, '\n'))
	}
	if err != nil {
		l.err = fmt.Errorf("write trace log: %w", err)
	}
}

// bodyFields includes readable text and base64 bytes so even non-UTF-8 chunks
// can be inspected without corrupting the JSON-lines log.
func (l *traceLogger) bodyFields(b []byte) map[string]any {
	if l.key != "" {
		b = bytes.ReplaceAll(b, []byte(l.key), []byte("[REDACTED]"))
	}
	return map[string]any{"text": string(b), "bytes_base64": b, "size": len(b)}
}

// headers copies HTTP metadata while hiding authentication and cookie values.
func headers(h http.Header) http.Header {
	result := h.Clone()
	for name := range result {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "authorization") || strings.Contains(lower, "api-key") || strings.Contains(lower, "apikey") || strings.Contains(lower, "token") || strings.Contains(lower, "secret") || strings.Contains(lower, "cookie") {
			result[name] = []string{"[REDACTED]"}
		}
	}
	return result
}

// traceBody observes bytes as the SDK consumes them without buffering the stream
// ahead of the SDK. Response lines expose SSE framing before event decoding.
type traceBody struct {
	io.ReadCloser
	log       *traceLogger
	direction string
	pending   []byte
	finished  bool
}

func (b *traceBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.log.record(b.direction+".body.chunk", b.log.bodyFields(p[:n]))
		if b.direction == "response" {
			b.pending = append(b.pending, p[:n]...)
			for {
				i := bytes.IndexByte(b.pending, '\n')
				if i < 0 {
					break
				}
				line := b.pending[:i+1]
				fields := b.log.bodyFields(line)
				trimmed := strings.TrimRight(string(line), "\r\n")
				kind := "field"
				switch {
				case trimmed == "":
					kind = "frame_boundary"
				case strings.HasPrefix(trimmed, ":"):
					kind = "comment"
				case strings.HasPrefix(trimmed, "data:"):
					kind = "data"
				case strings.HasPrefix(trimmed, "event:"):
					kind = "event_type"
				}
				fields["kind"] = kind
				b.log.record("parse.sse.line", fields)
				b.pending = b.pending[i+1:]
			}
		}
	}
	if err != nil && !b.finished {
		b.finished = true
		b.flush()
		b.log.record(b.direction+".body.end", map[string]any{"error": err.Error()})
	}
	return n, err
}

// flush records an unterminated line, including when completion closes early.
func (b *traceBody) flush() {
	if len(b.pending) > 0 {
		b.log.record("parse.sse.tail", b.log.bodyFields(b.pending))
		b.pending = nil
	}
}

func (b *traceBody) Close() error {
	b.flush()
	err := b.ReadCloser.Close()
	fields := map[string]any{}
	if err != nil {
		fields["error"] = err.Error()
	}
	b.log.record(b.direction+".body.close", fields)
	return err
}

// failure safely reads a logging error after request and parsing work finishes.
func (l *traceLogger) failure() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}
