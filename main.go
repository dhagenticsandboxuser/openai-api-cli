// openai-api-cli is a basic text prediction tester for the OpenAI Responses API.
package main

import (
	"bufio"
	"bytes"
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
	"time"
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
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
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
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
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
	keyBytes, err := os.ReadFile(*credentials)
	if err != nil {
		return fmt.Errorf("read credentials: %w", err)
	}
	key := strings.TrimSpace(string(keyBytes))
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
	// Additional parameters permit testing API options without adding dependencies.
	params := make(map[string]any)
	if *paramsFile != "" {
		b, err := os.ReadFile(*paramsFile)
		if err != nil {
			return fmt.Errorf("read parameters: %w", err)
		}
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
	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if *project != "" {
		req.Header.Set("OpenAI-Project", *project)
	}
	if *organization != "" {
		req.Header.Set("OpenAI-Organization", *organization)
	}
	// Refuse redirects to avoid forwarding credentials to another endpoint.
	client := &http.Client{Timeout: time.Duration(*timeout), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if err != nil {
			return fmt.Errorf("HTTP %s (cannot read error body)", resp.Status)
		}
		return fmt.Errorf("HTTP %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return stream(resp.Body, stdout)
}

// stream decodes SSE frames, writes text/refusal deltas immediately, and requires
// a terminal API event. EOF alone is an interrupted prediction, not success.
func stream(r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	var data []string
	dispatch := func() (bool, error) {
		if len(data) == 0 {
			return false, nil
		}
		payload := strings.Join(data, "\n")
		data = nil
		var event struct {
			Type     string `json:"type"`
			Delta    string `json:"delta"`
			Message  string `json:"message"`
			Response struct {
				Error *struct {
					Message string `json:"message"`
				} `json:"error"`
				IncompleteDetails *struct {
					Reason string `json:"reason"`
				} `json:"incomplete_details"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return false, fmt.Errorf("decode stream event: %w", err)
		}
		switch event.Type {
		case "response.output_text.delta", "response.refusal.delta":
			if _, err := io.WriteString(w, event.Delta); err != nil {
				return false, fmt.Errorf("write output: %w", err)
			}
		case "response.completed":
			return true, nil
		case "response.failed", "response.incomplete", "response.cancelled", "error":
			detail := event.Message
			if event.Response.Error != nil {
				detail = event.Response.Error.Message
			}
			if event.Response.IncompleteDetails != nil {
				detail = event.Response.IncompleteDetails.Reason
			}
			return false, fmt.Errorf("%s: %s", event.Type, detail)
		}
		return false, nil
	}
	// SSE comments and other fields are ignored; multiple data lines form one event.
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			done, err := dispatch()
			if err != nil || done {
				return err
			}
		} else if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read stream: %w", err)
	}
	done, err := dispatch()
	if err != nil || done {
		return err
	}
	return errors.New("stream ended before a terminal response event")
}
