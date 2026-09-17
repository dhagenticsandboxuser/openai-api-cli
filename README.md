# openai-api-cli

A basic testing tool written in Go for direct text predictions with the
[OpenAI Responses API](https://developers.openai.com/api/reference/cli/resources/responses/methods/create).
It sends one prompt and streams generated text (including refusal text) to stdout
until the API terminates the response. It uses only the Go standard library.
This is a small testing utility, with no chat history, tool execution, or retries.

## Build and run

Requires Go 1.22 or later.

```sh
go build -o openai-api-cli .
printf 'Explain Go interfaces briefly.\n' | ./openai-api-cli \
  -model YOUR_MODEL_ID -credentials /path/to/api-key.txt

./openai-api-cli -model YOUR_MODEL_ID \
  -credentials /path/to/credentials.json -prompt-file /path/to/prompt.txt
```

The credentials file contains either a plain API key (surrounding whitespace is
trimmed) or JSON such as `{"api_key":"YOUR_API_KEY"}`. Keep it outside the repository
and restrict its permissions, for example with `chmod 600`. There are no implicit
environment-variable credentials or settings; all configuration uses CLI flags.
The default prompt source is stdin. Use `-help` to show every option.

| Flag | Purpose / default |
| --- | --- |
| `-model` | Required model ID available to your account |
| `-credentials` | Required path to plain-key or JSON credentials |
| `-prompt-file` | Prompt path; `-` reads stdin (default) |
| `-endpoint` | Full URL; default `https://api.openai.com/v1/responses` |
| `-instructions` | Optional model instructions |
| `-project` | Optional OpenAI project ID header |
| `-organization` | Optional OpenAI organization ID header |
| `-max-output-tokens` | Positive output token cap; `0` uses API default |
| `-timeout` | Total request duration such as `2m`; `0` has no timeout |
| `-params-file` | Optional JSON object of additional API parameters |

For example, a params file can contain `{"temperature":0.7}` when supported by
the selected model. The CLI always overrides `model`, `input`, `stream`, and
`background`; explicit instructions/token-limit flags override those JSON fields.
`store` defaults to false unless supplied in the params file. Only text and refusal
deltas are printed; other events are consumed but not displayed.

Output is written immediately without an added newline. Diagnostics go to stderr.
Successful completion exits 0. API failure, incomplete output (including reaching
an output token limit), unexpected disconnects, output errors, or interruption
exit nonzero; partial output may already have been printed. Ctrl-C cancels the
request. There are no automatic continuation requests after termination. The
endpoint flag supports local mock servers; real credentials should be sent only
to a trusted HTTPS endpoint. Redirects are refused.

## Validation

```sh
go test ./...
go vet ./...
```

Tests use local HTTP servers and dummy credentials to check requests, SSE framing,
streamed output, and failure handling. They do not contact OpenAI or incur charges.
A live API smoke test requires your own valid credentials and incurs normal API
usage charges; it has not been performed as part of this implementation.
