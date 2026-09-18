package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestVerboseLogging verifies raw traffic, parser steps, credential redaction,
// and identical prediction output through the real SDK and a local HTTP server.
func TestVerboseLogging(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "key")
	if err := os.WriteFile(key, []byte("dummy-secret-key"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Set-Cookie", "session=secret")
		io.WriteString(w, ": heartbeat\n\ndata: {\"type\":\"response.created\"}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\ndata: {\"type\":\"response.completed\"}\n\n")
	}))
	defer server.Close()
	for _, fileLog := range []bool{false, true} {
		var output, diagnostics bytes.Buffer
		args := []string{"-model", "test", "-credentials", key, "-endpoint", server.URL}
		path := filepath.Join(dir, "trace.jsonl")
		if fileLog {
			args = append(args, "-log-file", path)
		} else {
			args = append(args, "-verbose")
		}
		if err := run(context.Background(), args, strings.NewReader("test prompt"), &output, &diagnostics); err != nil {
			t.Fatal(err)
		}
		if output.String() != "hello" {
			t.Fatalf("output: %q", output.String())
		}
		logs := diagnostics.Bytes()
		if fileLog {
			if diagnostics.Len() != 0 {
				t.Fatal("file logs leaked onto stderr")
			}
			var err error
			logs, err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0600 {
				t.Errorf("log permissions: %v", info.Mode())
			}
		}
		if bytes.Contains(logs, []byte("dummy-secret-key")) || bytes.Contains(logs, []byte("session=secret")) {
			t.Fatal("credentials leaked")
		}
		steps := map[string]bool{}
		seq := 0
		var traffic strings.Builder
		for _, line := range bytes.Split(bytes.TrimSpace(logs), []byte("\n")) {
			var entry struct {
				Step     string
				Sequence int
				Text     string
			}
			if err := json.Unmarshal(line, &entry); err != nil {
				t.Fatal(err)
			}
			seq++
			if entry.Sequence != seq {
				t.Errorf("sequence: %d want %d", entry.Sequence, seq)
			}
			steps[entry.Step] = true
			if strings.HasSuffix(entry.Step, "body.chunk") {
				traffic.WriteString(entry.Text)
			}
		}
		for _, step := range []string{"flags.parsed", "credentials.parse", "prompt.read", "parameters.resolved", "request.send", "request.body.chunk", "response.receive", "response.body.chunk", "parse.sse.line", "parse.event.next", "parse.event.decoded", "parse.event.ignored", "output.write", "parse.event.completed", "run.end"} {
			if !steps[step] {
				t.Errorf("missing step: %s", step)
			}
		}
		if !strings.Contains(traffic.String(), "test prompt") || !strings.Contains(traffic.String(), "response.completed") {
			t.Fatal("raw bodies missing")
		}
	}
	var prediction bytes.Buffer
	err := run(context.Background(), []string{"-model", "test", "-credentials", key, "-endpoint", server.URL, "-verbose"}, strings.NewReader("test prompt"), &prediction, brokenWriter{})
	if err == nil || !strings.Contains(err.Error(), "write trace log") {
		t.Fatalf("logging failure: %v", err)
	}

}

// TestTraceFailure makes a broken log destination fail the command's trace.
func TestTraceFailure(t *testing.T) {
	log := &traceLogger{writer: brokenWriter{}}
	log.record("test", nil)
	if log.failure() == nil {
		t.Fatal("logging error was hidden")
	}
}

// TestSplitSSELines verifies line tracing across fragmented network reads.
func TestSplitSSELines(t *testing.T) {
	var output bytes.Buffer
	log := &traceLogger{writer: &output}
	body := &traceBody{ReadCloser: io.NopCloser(strings.NewReader("data: test\r\n\n")), log: log, direction: "response"}
	buffer := make([]byte, 2)
	for {
		_, err := body.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "frame_boundary") || !strings.Contains(output.String(), `data: test\r\n`) {
		t.Fatal("missing line trace")
	}
}
