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

## License

Apache License 2.0. See [LICENSE](./LICENSE).
