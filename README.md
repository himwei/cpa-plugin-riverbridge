# RiverBridge

A [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) plugin that turns
[River AI](https://docs.river.ai) inference into a first-class CPA provider.

Add a River API key once; RiverBridge publishes River's models into CPA's
`/v1/models` namespace prefixed with `river-`, routes chat completions upstream,
and accounts tokens and cost per key — all from a small web console served by
CPA itself.

```
river-deepseek-v4.1-flash-1017   deepseek-ai/DeepSeek-V4.1-Flash-1017
river-glm-5.3-flash              zai-org/GLM-5.3-Flash
river-kimi-k3                    moonshotai/Kimi-K3
```

## Why the `river-` prefix

CPA shares one flat model namespace across every provider. River serves models
whose short names collide with other providers' models — `deepseek-v4.1-flash`
exists elsewhere too. Every model RiverBridge registers is therefore prefixed
with `river-`, so `river-deepseek-v4.1-flash` is unambiguously River's.

The prefix is applied to the slug **after** the vendor segment is dropped:
River's `deepseek-ai/DeepSeek-V4.1-Flash-1017` becomes
`river-deepseek-v4.1-flash-1017`. The mapping is reversed on the way back
upstream, so the original id is always what River receives.

## Install

RiverBridge is a C shared object, exactly like every other CPA plugin.

```sh
# FreeBSD (the usual CPA host)
gmake freebsd

# copy the artifact into the plugin scan directory
scp dist/riverbridge.so user@host:~/cpa/plugins/freebsd/amd64/riverbridge.so
```

Then enable it in `config.yaml`:

```yaml
plugins:
  enabled: true
  dir: "plugins"
  configs:
    riverbridge:
      enabled: true
      base_url: "https://api.river.ai/v1"
      request_timeout_seconds: 600
      log_retention: 500
```

Restart CPA and look for the registration line:

```
pluginhost: plugin registered plugin_id=riverbridge plugin_name=RiverBridge version=0.1.0
```

## Use

Open the console from CPA's management UI — RiverBridge registers a
**RiverBridge** entry under the plugin resources — or directly at:

```
/v0/resource/plugins/riverbridge/console
```

Paste a River key (`rv_...`) and RiverBridge probes it upstream before writing
anything to disk, so an unusable key never reaches the auth directory. The key
becomes a normal CPA auth record; the console shows its models, token usage and
accumulated cost.

The console's API calls go to `/v0/management/riverbridge/*`, which CPA guards
with the management key. If you are logged into CPA's management center with
"remember credentials" on, RiverBridge reuses that session; otherwise it shows
an unlock dialog.

Then chat through CPA as usual:

```sh
curl http://127.0.0.1:56466/v1/chat/completions \
  -H "Authorization: Bearer $CPA_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"river-deepseek-v4.1-flash-1017","messages":[{"role":"user","content":"hi"}]}'
```

## Configuration

Every field lives under `plugins.configs.riverbridge` and can be overridden at
runtime through the console, which persists to
`plugins/riverbridge-data/settings.json`.

| Key | Default | Meaning |
| --- | --- | --- |
| `data_dir` | `plugins/riverbridge-data` | Plugin state (settings, usage) |
| `base_url` | `https://api.river.ai/v1` | Upstream for every credential |
| `log_retention` | `300` | Diagnostic log lines kept in memory |
| `request_timeout_seconds` | `600` | One upstream call's deadline |
| `include_stream_usage` | `true` | Ask River for usage on streaming calls |

## Streaming

Streaming calls are proxied frame by frame, not buffered. RiverBridge asks for
`stream_options.include_usage` so the final frame carries token counts, watches
for a missing `[DONE]`, and aborts a stream that stalls for five minutes so a
dead upstream cannot pin a connection open forever.

## Pricing

The built-in table (`internal/bridge/pricing.go`) mirrors River's published
rates so the console can show cost without another API call. Models added
upstream after a release cost `0` here rather than a guessed number.

**Note for CPA users running `cpa-key-billing`:** that plugin refuses requests
for models it has no price for (`model_price_error`, HTTP 503). After adding a
new River model, register its price once:

```sh
curl -X PUT http://127.0.0.1:56466/v0/management/plugins/cpa-key-billing/prices \
  -H "X-Management-Key: $MGMT" -H 'Content-Type: application/json' \
  -d '{"model_id":"river-deepseek-v4.1-flash","input_per_1m":0.12,"output_per_1m":0.48,"cache_read_per_1m":0.0024}'
```

## Build

```sh
go build ./...          # compile check
go test ./...           # unit tests
gmake freebsd           # C shared object for FreeBSD/amd64
```

Requires Go 1.26+ and, for the shared object, a C compiler
(`CGO_ENABLED=1`).

## Layout

```
cmd/riverbridge/main.go   C ABI surface CPA dlopen()s
internal/bridge/
  service.go              RPC dispatch (the plugin protocol)
  auth.go                 key → auth record, model registration
  discovery.go            /v1/models probing and validation
  executor.go             chat completion proxy, SSE relay
  pricing.go              River rate table
  management.go           console API + routes
  store.go                usage accounting, log ring
  ui/console.html         the console
```

## License

MIT — see [LICENSE](LICENSE).
