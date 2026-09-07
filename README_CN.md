# API HUB Panel 独立版

一个独立的 OpenAI 兼容多渠道 AI API 网关与管理面板。

## 核心能力

- 兼容 OpenAI API：`/v1/chat/completions`、`/v1/responses`、图片、Embedding、音频等接口
- 多渠道路由、权重、轮询、失败重试和渠道冷却
- 渠道可视化管理与上游模型一键拉取
- 模型别名映射，解决不同渠道同名模型冲突
- API Key 模型权限、有效期和启用状态管理
- Token 用量统计、渠道成功率和请求来源监控
- Rust 原生运行时，支持流式响应及多种上游协议
- PWA 管理面板、响应式布局、亮色/暗色主题
- 渠道级自定义请求头，可为每个上游单独附加 Header

## 独立性说明

这是一个独立维护的项目仓库：

- 不包含 Git submodule
- 不包含 fork 自动同步工作流
- 不依赖其他 fork 仓库提供运行时代码
- Docker 镜像由本仓库自己的 GitHub Actions 构建并发布到 GHCR

项目保留 Apache License 2.0 许可文件，详情见 [LICENSE](./LICENSE)。

## 快速部署

### 1. 准备配置

```bash
cp api.yaml.example api.yaml
```

编辑 `api.yaml`，至少配置一个管理员 Key 和一个上游渠道。推荐通过环境变量引用密钥：

```yaml
api_keys:
  - api: ${ADMIN_API_KEY}
    role: admin

providers:
  - provider: openai
    base_url: https://api.openai.com/v1
    api: ${OPENAI_API_KEY}
    model:
      - gpt-4o
      # 上游真实模型名: 对外暴露的别名
      # - gpt-4o: gpt-4o-openai
```

不要把包含真实密钥的 `api.yaml` 或 `.env` 文件提交到 Git。

### 使用 GHCR 镜像

```bash
docker login ghcr.io
docker pull ghcr.io/pinevu/uni-api-panel:latest
docker compose -f docker-compose.release.yml up -d
```

默认访问：

- 管理面板：`http://localhost:3000/`
- API：`http://localhost:3000/v1/`

### 3. 从源码构建

```bash
docker build -t api-hub-panel:latest .
docker compose -f docker-compose.release.yml up -d
```

## 渠道自定义请求头

可在管理面板的渠道编辑器中配置固定的上游请求头，每行一个 `Header-Name: value`。也可以直接写入配置文件：

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

请求发送到该渠道时会附加这些 Header；同名的内置请求头会以渠道配置为准。不要在仓库中保存真实 Token、Cookie 或其他敏感值。


在渠道的 `model` 或 `models` 列表中使用映射格式：

```yaml
providers:
  - provider: cpa
    base_url: https://example.com/v1
    api: ${CPA_API_KEY}
    model:
      - gpt-4o: gpt-4o-cpa
      - claude-sonnet-4-6: claude-cpa
```

客户端调用 `gpt-4o-cpa` 时，网关会把模型字段还原为上游的 `gpt-4o`，其他请求参数和响应协议保持不变。

管理面板支持：

- 单个模型设置、修改和清除别名
- 为已选模型批量添加前缀
- 文本输入 `upstream: alias` 或 `upstream -> alias`
- 重新拉取上游模型时保留已有选择状态

## 开发与测试

```bash
uv sync
cargo test --manifest-path rust/uni-api-native/Cargo.toml --locked
uv run pytest -q
```

## 目录结构

- `static/`：API HUB 管理面板
- `rust/uni-api-native/`：Rust 原生运行时、路由及协议适配
- `uni_api/`：Python 兼容层和上游适配
- `api.yaml.example`：无密钥配置示例
- `Dockerfile`：多阶段构建文件
- `.github/workflows/`：本仓库自己的测试、镜像和 Release 工作流
