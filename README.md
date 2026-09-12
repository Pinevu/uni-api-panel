# API HUB Panel

独立的 OpenAI-compatible multi-provider AI API gateway and management panel.

## Highlights

- OpenAI-compatible chat, responses, image, embedding, audio and model APIs
- Multi-provider routing with weights, round-robin, retries and cooldowns
- Visual channel management, upstream model discovery and model aliases
- API key model permissions, expiry and enable/disable controls
- Token usage, provider success-rate and request-source monitoring
- Rust native runtime with streaming and multiple provider adapters
- Responsive PWA management panel with light and dark themes
- Per-provider custom upstream headers

This repository is standalone: it has no Git submodule, fork-sync workflow or runtime dependency on another fork. Container images are built by this repository and published to GitHub Container Registry.

## Quick start

```bash
cp api.yaml.example api.yaml
# edit api.yaml and provide your own secrets
docker login ghcr.io
docker pull ghcr.io/pinevu/uni-api-panel:latest
docker compose -f docker-compose.release.yml up -d
```

The panel listens on port `3000` by default. See [README_CN.md](./README_CN.md) for configuration and model-alias examples.

## Per-provider custom headers

The channel editor accepts one fixed upstream header per line in `Header-Name: value` format. The same setting can be written in `api.yaml`:

```yaml
providers:
  - provider: openai
    base_url: https://api.openai.com/v1
    api: ${OPENAI_API_KEY}
    preferences:
      headers:
        X-Provider-Route: fast
        X-Client-Version: api-hub-panel
    model:
      - gpt-4o
```

These headers are added to requests sent to that provider. When a configured header has the same name as a built-in header, the provider configuration wins. Do not store real tokens, cookies, or other secrets in the repository.

## Prompt cache affinity

For upstreams verified to support prompt caching, enable **Prompt Cache Affinity** in the channel editor. It does not cache or replay model answers. Instead, it adds a stable `prompt_cache_key` for the same client/model and keeps multi-key traffic on one healthy upstream key until that key cools down.

```yaml
providers:
  - provider: cpa
    base_url: https://example.com/v1
    api: ${CPA_API_KEY}
    preferences:
      prompt_cache_affinity: true
      prompt_cache_retention: 24h
    model:
      - gpt-5.6-luna
```

The feature is disabled by default. Use it only with upstreams that accept the relevant cache fields; short prompts may legitimately report zero cached tokens.

## License

Apache License 2.0. See [LICENSE](./LICENSE).
