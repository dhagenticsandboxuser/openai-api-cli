// openai-api-cli is a basic text prediction tester for the OpenAI Responses API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/ssestream"
	"github.com/openai/openai-go/v3/responses"
)

// main cancels an active request on interrupt and keeps diagnostics off stdout.
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "openai-api-cli:", err)
		os.Exit(1)
	}
}

// run parses all configuration from flags, loads credentials and the prompt,
// then makes exactly one streaming prediction request (no automatic retries).
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (result error) {
	fs := flag.NewFlagSet("openai-api-cli", flag.ContinueOnError)
	fs.SetOutput(stderr)
	model := fs.String("model", "", "Required model ID")
	credentials := fs.String("credentials", "", "Required file containing an API key or JSON {\"api_key\":\"...\"}")
	promptFile := fs.String("prompt-file", "-", "Prompt file; - reads stdin")
	endpoint := fs.String("endpoint", "https://api.openai.com/v1/responses", "Responses API URL")
	instructions := fs.String("instructions", "", "Optional model instructions")
	project := fs.String("project", "", "Optional OpenAI project ID")
	organization := fs.String("organization", "", "Optional OpenAI organization ID")
	maxTokens := fs.Int("max-output-tokens", 0, "Output token limit; 0 uses the API default")
	timeout := fs.Duration("timeout", 0, "Total request timeout, e.g. 2m; 0 waits until termination")
	paramsFile := fs.String("params-file", "", "Optional JSON object of additional Responses API parameters")
	verbose := fs.Bool("verbose", false, "Log HTTP traffic and parsing steps as JSON lines to stderr")
	logFile := fs.String("log-file", "", "Write verbose JSON-lines logs to this file (enables logging)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	log := &traceLogger{}
	if *verbose {
		log.writer = stderr
	}
	if *logFile != "" {
		file, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("open trace log: %w", err)
		}
		log.writer = file
		defer func() {
			if err := file.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("close trace log: %w", err))
			}
		}()
	}
	defer func() {
		fields := map[string]any{"success": result == nil}
		if result != nil {
			fields["error"] = result.Error()
		}
		log.record("run.end", fields)
		result = errors.Join(result, log.failure())
	}()
	log.record("flags.parsed", map[string]any{"model": *model, "credentials_path": *credentials, "prompt_file": *promptFile, "endpoint": *endpoint, "params_file": *paramsFile, "timeout": timeout.String(), "max_output_tokens": *maxTokens})
	log.record("configuration.validate", nil)
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *model == "" || *credentials == "" {
		return errors.New("-model and -credentials are required; use -help for options")
	}
	if *maxTokens < 0 || *timeout < 0 {
		return errors.New("token limit and timeout must be nonnegative")
	}
	u, err := url.Parse(*endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return errors.New("-endpoint must be an HTTP(S) URL without user credentials")
	}
	log.record("credentials.read", map[string]any{"path": *credentials})
	keyBytes, err := os.ReadFile(*credentials)
	if err != nil {
		return fmt.Errorf("read credentials: %w", err)
	}
	key := strings.TrimSpace(string(keyBytes))
	log.record("credentials.parse", map[string]any{"format": map[bool]string{true: "json", false: "plain"}[strings.HasPrefix(key, "{")]})
	if strings.HasPrefix(key, "{") {
		var c struct {
			APIKey string `json:"api_key"`
		}
		if json.Unmarshal(keyBytes, &c) != nil {
			return errors.New("invalid credentials JSON")
		}
		key = strings.TrimSpace(c.APIKey)
	}
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return errors.New("credentials must contain one nonempty API key")
	}
	log.key = key
	log.record("credentials.validated", nil)
	log.record("prompt.read", map[string]any{"source": *promptFile})
	var prompt []byte
	if *promptFile == "-" {
		prompt, err = io.ReadAll(stdin)
	} else {
		prompt, err = os.ReadFile(*promptFile)
	}
	if err != nil {
		return fmt.Errorf("read prompt: %w", err)
	}
	if strings.TrimSpace(string(prompt)) == "" {
		return errors.New("prompt is empty")
	}
	log.record("prompt.validated", map[string]any{"size": len(prompt)})
	// Additional parameters permit testing options beyond the dedicated CLI flags.
	params := make(map[string]any)
	if *paramsFile != "" {
		b, err := os.ReadFile(*paramsFile)
		if err != nil {
			return fmt.Errorf("read parameters: %w", err)
		}
		log.record("parameters.parse", log.bodyFields(b))
		if json.Unmarshal(b, &params) != nil || params == nil {
			return errors.New("parameters must be a JSON object")
		}
	}
	// These fields are owned by the CLI so it always receives a synchronous stream.
	params["model"], params["input"], params["stream"], params["background"] = *model, string(prompt), true, false
	if _, ok := params["store"]; !ok {
		params["store"] = false
	}
	if *instructions != "" {
		params["instructions"] = *instructions
	}
	if *maxTokens > 0 {
		params["max_output_tokens"] = *maxTokens
	}
	encodedParams, _ := json.Marshal(params)
	log.record("parameters.resolved", log.bodyFields(encodedParams))
	// A context deadline covers the entire stream; SDK retries are explicitly disabled.
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	// The SDK Responses service avoids inheriting environment settings. Retain the
	// full endpoint flag with middleware, since the SDK normally appends /responses.
	service := responses.NewResponseService(
		option.WithAPIKey(key),
		option.WithBaseURL(u.Scheme+"://"+u.Host+"/"),
		option.WithProject(*project),
		option.WithOrganization(*organization),
		option.WithMaxRetries(0),
		option.WithHTTPClient(&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}),
		option.WithMiddleware(func(req *http.Request, next option.MiddlewareNext) (*http.Response, error) {
			endpointURL := *u
			req.URL = &endpointURL
			if log.writer != nil {
				log.record("request.send", map[string]any{"method": req.Method, "url": req.URL.String(), "headers": headers(req.Header)})
				if req.Body != nil {
					req.Body = &traceBody{ReadCloser: req.Body, log: log, direction: "request"}
				}
			}
			resp, err := next(req)
			if err != nil {
				log.record("request.error", map[string]any{"error": err.Error()})
			}
			if resp != nil && log.writer != nil {
				log.record("response.receive", map[string]any{"status": resp.StatusCode, "headers": headers(resp.Header)})
				if resp.Body != nil {
					resp.Body = &traceBody{ReadCloser: resp.Body, log: log, direction: "response"}
				}
			}
			return resp, err
		}),
	)
	request := responses.ResponseNewParams{
		Model: *model,
		Input: responses.ResponseNewParamsInputUnion{OfString: openai.String(string(prompt))},
	}
	// SDK JSON options preserve arbitrary parameters and the CLI override rules.
	opts := []option.RequestOption{option.WithHeader("Accept", "text/event-stream")}
	for name, value := range params {
		opts = append(opts, option.WithJSONSet(name, value))
	}
	log.record("sdk.stream.open", nil)
	events := service.NewStreaming(ctx, request, opts...)
	defer events.Close()
	return stream(events, stdout, log)
}

// stream consumes SDK-decoded events and writes text/refusal deltas immediately.
// EOF alone is an interrupted prediction, not successful completion.
func stream(events *ssestream.Stream[responses.ResponseStreamEventUnion], w io.Writer, logs ...*traceLogger) error {
	var log *traceLogger
	if len(logs) > 0 {
		log = logs[0]
	}
	for {
		log.record("parse.event.next", nil)
		if !events.Next() {
			break
		}
		event := events.Current()
		log.record("parse.event.decoded", map[string]any{"type": event.Type, "sequence_number": event.SequenceNumber})
		switch event.Type {
		case "response.output_text.delta", "response.refusal.delta":
			log.record("output.write", map[string]any{"size": len(event.Delta)})
			if _, err := io.WriteString(w, event.Delta); err != nil {
				return fmt.Errorf("write output: %w", err)
			}
		case "response.completed":
			log.record("parse.event.completed", nil)
			return nil
		case "response.failed", "response.incomplete", "response.cancelled", "error":
			detail := event.Message
			if event.Response.Error.Message != "" {
				detail = event.Response.Error.Message
			}
			if event.Response.IncompleteDetails.Reason != "" {
				detail = string(event.Response.IncompleteDetails.Reason)
			}
			log.record("parse.event.failure", map[string]any{"type": event.Type, "detail": detail})
			return fmt.Errorf("%s: %s", event.Type, detail)
		default:
			log.record("parse.event.ignored", map[string]any{"type": event.Type})
		}
	}
	log.record("parse.event.end", nil)
	if err := events.Err(); err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	return errors.New("stream ended before a terminal response event")
}
