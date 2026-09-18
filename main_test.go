package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openai/openai-go/v3/packages/ssestream"
	"github.com/openai/openai-go/v3/responses"
)

// TestRun exercises credentials, prompt input, request fields, and streamed output
// through a real local HTTP server; it never contacts OpenAI.
func TestRun(t *testing.T) {
	// SDK environment defaults must not override explicit CLI configuration.
	t.Setenv("OPENAI_API_KEY", "wrong-environment-key")
	t.Setenv("OPENAI_PROJECT_ID", "wrong-project")
	t.Setenv("OPENAI_ORG_ID", "wrong-organization")
	t.Setenv("OPENAI_BASE_URL", "http://127.0.0.1:1")
	t.Setenv("OPENAI_CUSTOM_HEADERS", "X-Environment-Header: unwanted")
	for _, filePrompt := range []bool{false, true} {
		t.Run(map[bool]string{false: "stdin", true: "file"}[filePrompt], func(t *testing.T) {
			dir := t.TempDir()
			keyPath := filepath.Join(dir, "key.json")
			if err := os.WriteFile(keyPath, []byte(`{"api_key":"dummy-test-key"}`), 0600); err != nil {
				t.Fatal(err)
			}
			paramsPath := filepath.Join(dir, "params.json")
			if err := os.WriteFile(paramsPath, []byte(`{"temperature":0.5,"stream":false,"background":true}`), 0600); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer dummy-test-key" {
					t.Error("incorrect request authentication or method")
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body["input"] != "hello" || body["model"] != "test-model" || body["stream"] != true || body["background"] != false || body["max_output_tokens"] != float64(10) || body["temperature"] != 0.5 {
					t.Errorf("incorrect body: %v", body)
				}
				if r.URL.RequestURI() != "/custom/predict?test=1" {
					t.Errorf("endpoint: %s", r.URL.RequestURI())
				}
				if r.Header.Get("OpenAI-Organization") != "" || r.Header.Get("X-Environment-Header") != "" {
					t.Error("inherited environment headers")
				}
				if r.Header.Get("OpenAI-Project") != "project" {
					t.Error("missing project header")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				// Flush each delta before completion to exercise incremental HTTP streaming.
				for _, frame := range []string{
					"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello \"}\n\n",
					"data: {\"type\":\"response.output_text.delta\",\"delta\":\"world\"}\n\n",
					"data: {\"type\":\"response.completed\"}\n\n",
				} {
					io.WriteString(w, frame)
					w.(http.Flusher).Flush()
				}
			}))
			defer server.Close()
			args := []string{"-model", "test-model", "-credentials", keyPath, "-endpoint", server.URL + "/custom/predict?test=1", "-params-file", paramsPath, "-max-output-tokens", "10", "-project", "project"}
			if filePrompt {
				path := filepath.Join(dir, "prompt")
				if err := os.WriteFile(path, []byte("hello"), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "-prompt-file", path)
			}
			var output bytes.Buffer
			if err := run(context.Background(), args, strings.NewReader("hello"), &output, io.Discard); err != nil {
				t.Fatal(err)
			}
			if output.String() != "hello world" {
				t.Fatalf("output: %q", output.String())
			}
		})
	}
}

// TestStream checks SSE framing and distinguishes API termination from lost data.
func TestStream(t *testing.T) {
	cases := []struct{ name, input, want, err string }{
		{"multiline CRLF", ": keepalive\r\nevent: ignored\r\ndata: {\"type\":\"response.output_text.delta\",\r\ndata: \"delta\":\"ok\"}\r\n\r\ndata: {\"type\":\"response.completed\"}\r\n\r\n", "ok", ""},
		{"refusal", "data: {\"type\":\"response.refusal.delta\",\"delta\":\"no\"}\n\ndata: {\"type\":\"response.completed\"}\n\n", "no", ""},
		{"truncated", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n", "partial", "before a terminal"},
		{"incomplete", "data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n", "", "max_output_tokens"},
		{"failed", "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"failure\"}}}\n\n", "", "failure"},
		{"error", "data: {\"type\":\"error\",\"message\":\"bad request\"}\n\n", "", "bad request"},
		{"malformed", "data: invalid\n\n", "", "stream:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var output bytes.Buffer
			err := stream(testEvents(c.input), &output)
			if output.String() != c.want {
				t.Errorf("output: %q", output.String())
			}
			if c.err == "" && err != nil || c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)) {
				t.Errorf("error: %v", err)
			}
		})
	}
}

// brokenWriter simulates a closed stdout pipe.
type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("closed pipe") }

// TestOutputFailure ensures writing errors are propagated instead of hidden.
func TestOutputFailure(t *testing.T) {
	err := stream(testEvents("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n"), brokenWriter{})
	if err == nil || !strings.Contains(err.Error(), "closed pipe") {
		t.Fatalf("error: %v", err)
	}
}

// TestHTTPFailure ensures an unsuccessful HTTP response fails the command.
func TestHTTPFailure(t *testing.T) {
	key := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(key, []byte("dummy"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "invalid key", 401) }))
	defer server.Close()
	err := run(context.Background(), []string{"-model", "test", "-credentials", key, "-endpoint", server.URL}, strings.NewReader("hello"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("error: %v", err)
	}
}

// testEvents uses the SDK decoder so fixtures exercise the same stream as HTTP.
func testEvents(input string) *ssestream.Stream[responses.ResponseStreamEventUnion] {
	response := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(input))}
	return ssestream.NewStream[responses.ResponseStreamEventUnion](ssestream.NewDecoder(response), nil)
}

// TestNoRetries keeps the testing tool from issuing extra billable predictions.
func TestNoRetries(t *testing.T) {
	key := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(key, []byte("dummy"), 0600); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	defer server.Close()
	err := run(context.Background(), []string{"-model", "test", "-credentials", key, "-endpoint", server.URL}, strings.NewReader("hello"), io.Discard, io.Discard)
	if err == nil || requests != 1 {
		t.Fatalf("error=%v requests=%d", err, requests)
	}
}
