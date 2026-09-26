# 13-access（L1）

> 状态：B1 评审中 | 层：第 1 层数据面 | 批次：B3
> 上游：[00-总纲](00-总纲.md) §6.1 / §6.2 / §3.1 | 工程基线：[01-工程规范](01-工程规范.md)

---

## 1. 模块边界

**做什么**：APIKey 鉴权（Bearer → Principal）；共享内核 `internal/provider` 的类型冻结与 OpenAI 适配器；`/v1/chat/completions` 与 `/v1/models` 接入 handler；用户 Key 管理 API（`/api/v1/user/keys`）。

**不做什么**：配额判断（17）；候选选择与容灾（16）；流转发机制（15）。`/v1` handler 只做编排：auth → precheck → route → execute（见 §3.4）。

**L2 调整**：相对 00 §4.1 预排增加 **13.4（用户 Key 管理 API）**。理由：Key 的创建/吊销/列表属 access 域（鉴权与 Key 生命周期是一体的），原 3 个 L2 装不下，且该 API 是 19.1 用户控制台的直接数据源。

## 2. 接口

- 跨模块契约：00 §6.1（共享内核，字段级在 13.2 冻结）、§6.2（`Authenticator` / `Principal`）。
- HTTP：`POST /v1/chat/completions`、`GET /v1/models`（返回启用别名列表）、`/api/v1/user/keys` CRUD。

## 3. 关键设计决策

1. **Key 指纹方案**：生成 `sk-` + 32B 随机；库中存 SHA-256 hash + 前 8 位前缀作索引列。鉴权 = 前缀定位候选行 + hash 比对。明文仅创建时返回一次（01 §8）。
2. **OpenAI 适配器近透传**：网关 → 上游的请求体走字段白名单透传，**流式时强制注入 `stream_options.include_usage=true`**——metering 依赖末 chunk 的 usage；非流式响应原生带 usage。不透传的字段（如自造扩展头）一律丢弃，保持 OpenAI 兼容承诺。
3. **`/v1/models` 首版只列启用别名**，不做按用户过滤（配额维度后置）——控制首版复杂度。

## 4. 错误映射表（13.3 冻结，面试高频题）

| 来源 | 网关响应 | 理由 |
|---|---|---|
| 无 Key / Key 格式错 | 401 `gaigate_unauthorized` | |
| Key 不存在 / hash 不符 | 401 `gaigate_unauthorized` | 不区分"不存在"与"错误"，防枚举 |
| Key 吊销 / 过期 | 403 `gaigate_key_revoked` / `gaigate_key_expired` | |
| 配额硬超限 | 429 `gaigate_quota_exceeded` + Retry-After | 17.2 返回 |
| 无可用候选 | 503 `gaigate_no_candidate` | 16 返回 |
| 上游 401/403 | **502** `gaigate_upstream_auth_failed` | 不透传 401——避免客户端误判是自己的 Key 问题 |
| 上游 429/5xx/超时 | 由 16.4 容灾消化；全部候选失败后 502 | 见 16-L1 |
| 流式中断 | 已写出部分保留，连接关闭，计该渠道失败 | 00 §3.2.1 |

## 5. L2 清单

- **13.1 鉴权中间件**：前缀查找 + hash 比对 + Principal 注入 ctx；单测覆盖上表前四行。
- **13.2 Provider 抽象与 OpenAI 适配器**：冻结 `ChatRequest/ChatResponse/StreamEvent` 字段清单；SSE 解析（`data:` 行、`[DONE]`、usage 提取）；`include_usage` 注入。
- **13.3 /v1 接入层**：handler 编排 + 错误映射表落地 + `GET /v1/models`。
- **13.4 用户 Key 管理 API**：创建（返回明文一次）/列表/吊销，供 19.1 复用。

## 6. 主要风险

- 强制注入 `include_usage` 可能被个别严格上游拒绝——白名单透传策略下这是唯一注入字段，文档化；遇到不支持上游时回退估算（17.1 已有该路径）。
- Key 前缀 8 字符 base62 的碰撞只影响候选行数（hash 最终裁决），无正确性风险。

## 7. 验收要点

B3 检查点贡献：无 Key 401；坏 Key 401；吊销 Key 403；正常 Key 经网关打到 mock 流式返回。
