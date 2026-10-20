# 13-access（L1）

> 状态：B3 修订候选，待用户 review（2026-10-20；尚未实现） | 层：第 1 层数据面 | 批次：B3
> 上游：[00-总纲](00-总纲.md) v0.9 候选 §5.2／§6／§8／§9 | 工程基线：[01-工程规范](01-工程规范.md)
> 本文及所属四份 L2 只作为 review 稿；用户批准后统一冻结，不能据草案开工。

## 1. 模块边界

负责用户 API Key 鉴权、共享 Provider 类型及 OpenAI HTTP 适配器、两个 /v1 端点、本人 Key 生命周期及首次创建命令。模型与请求结构只做网关必需的校验，未知正文内容保留；不承担真实配额、路由策略、健康、容灾或计量结算。

共享内核 `internal/provider` 只放类型／接口／分类错误；HTTP 实现在子包 `internal/provider/openai`。access 不 import router、meter、proxy；它定义消费方接口，由 cmd 注入实现。B3 临时编排归 `internal/bootstrap`，不是 access 的业务路由器。

## 2. HTTP 与 Go 边界

| 入口 | 身份与输出 |
|---|---|
| POST /v1/chat/completions | Bearer → Principal → 请求校验 → Precheck → ChatExecutor；流式 SSE 或非流式 JSON |
| GET /v1/models | 同一 Bearer；返回 `{"object":"list","data":[...]}`，列全部已配置别名 |
| GET／POST／DELETE /api/v1/user/keys[/:id] | B3 复用 Bearer 本人身份；19.1 后换为会话身份，业务服务只信标准 context 中的 Principal |
| cmd/g-aigate-key | 本地运维创建入口，显式用户名；仅成功时向 stdout 返回一次创建 JSON，不启动网关、不走 seed |

Authenticator／Principal／Provider 的权威签名在 00 §6.1／§6.2。access 的 ChatExecutor 接收请求而非 router.Plan；B5 需要装配适配器，不能声称与 router.Executor 签名相同。详见 13.3 §3。

## 3. 核心候选决策

1. **用户 Key 的完整闭环**：32 随机字节编码为无 padding 的 base64url，前置 sk-；raw 总长 46，前缀取完整 raw 的前 8 字节（含 sk-），SHA-256 hex 存库。前缀索引非唯一，碰撞后恒时比较完整摘要。无鉴权缓存，吊销提交后的新鉴权即失效；已放行的在途请求不撤销。
2. **请求保留结构**：messages 使用原始 JSON 对象列表，不把 content 收窄成 string；其余顶层字段进 Extra。适配器只重写 model、stream 和流式 include_usage，不原地修改传入请求。工具调用和多模态内容可透传，但 B3 不承诺其计量估算或对任意厂商兼容。
3. **终止必须显式**：SSE Chunk／UsageDone／Done／Error 区分；Done 对应一次 [DONE]。没有 Done 的 channel 关闭不是成功；usage 不是结束信号。Error 保留原因，响应已提交后入口异常终止真实 HTTP，避免正常 EOF 伪装成功。
4. **M0 与正式路由分期**：B3 仅直连最高优先级渠道的第一把启用 Key，无回退。precheck 是明确 AllowAll 桩；B5／B6 替换归属及验收登记台账，不能宣称已交付容灾或配额。
5. **不引入 HTTP 用户身份桩**：首次 Key 用本地命令创建；Key 管理 API 由有效 Bearer 标识本人。19.1 后接入会话／CSRF，不让请求参数或任意 header 指定 user_id。

## 4. 错误与提交边界

| 情况 | 响应未提交时 |
|---|---|
| 缺失／非法 Bearer、Key 不存在／hash 不符 | 401 gaigate_unauthorized |
| Key 吊销／过期、用户禁用 | 403 gaigate_key_revoked／gaigate_key_expired／gaigate_user_disabled |
| JSON／字段非法、正文超限、媒体类型不符 | 400 gaigate_invalid_request／413 gaigate_request_too_large／415 gaigate_unsupported_media_type |
| 别名不存在、无可用候选 | 404 gaigate_model_not_found／503 gaigate_no_candidate |
| store／装配内部故障 | 500 gaigate_internal，不伪装为 401 |
| 配额硬超限（未来消费者） | 429 gaigate_quota_exceeded + Retry-After: 60，后续 17.2 裁定精确等待值 |
| 上游 401／403 | 502 gaigate_upstream_auth_failed |
| 上游 429／5xx／网络／协议失败 | 502 gaigate_upstream_error；B3 不重试，B5 按策略消化 |
| 网关 TTFB／Overall 超时 | 502 gaigate_upstream_timeout |
| 上游 400／404／422 | 安全且完整的 OpenAI error 对象保留状态及 error 字段，补 request_id；不满足条件退回 502 |

已提交后不追加错误正文、不补 Done、不换渠道；入口以 http.ErrAbortHandler 中止，外层 Recover 必须透传该哨兵。客户端取消不再写错误。错误对象安全条件、错误体上限及敏感内容处理见 13.3，完整错误体与凭据不进日志。

## 5. L2 清单与施工顺序

| L2 | 交付 | 依赖／停点 |
|---|---|---|
| [13.1](13-access/13.1-auth-middleware.md) | 鉴权、身份 context、Key 生成、用户／Key 仓储、共用错误响应工具 | B2 已验收；M1 仓储与生成，M2 鉴权与中间件 |
| [13.2](13-access/13.2-provider-openai.md) | 共享类型、独立适配器、SSE 终止与有界 worker | 可在 13.1 后施工；M1 请求／非流式，M2 流与关闭 |
| [15.1](15-proxy/15.1-sse-forward.md)／[15.2](15-proxy/15.2-backpressure-cancel-timeout.md) | 转发、预算、慢写取消及机制测试 | 13.2 后依次交付 |
| [13.4](13-access/13.4-user-keys-api.md) | 本人 Key API、本地创建命令 | 只依赖 13.1；安排在 M0 集成前 |
| [13.3](13-access/13.3-v1-endpoints.md) | 接入、bootstrap、装配及完整 M0 | 以上全部验收合并后施工 |

相对 00 旧预排，13.4 已在旧 L1 拆出，本轮补齐 00 批次登记。先完成底层机制与首次签发，再做 13.3 的可运行集成，避免循环依赖和无凭据的“演示”。

## 6. 风险与验收

- 强制 include_usage 不做静默重试／降级；不支持的上游按失败报告，17.1 只在成功响应缺 usage 时估算。
- /v1/models 是配置发现，不保证当前健康、余额或请求一定成功；不添加 enabled 列或用户模型权限。
- 固定 10 MiB 入口正文上限由 13.3 实施；Gin multipart 内存配置不能替代请求体限制。
- B3 必须有正常 SSE 8 内容 + 1 usage + 1 Done、非流式、无 Key／吊销、真实断流、建连／响应头超时、取消与背压证据；限定测试、race、全仓真实 MySQL 回归与 lint 通过。未知字段用 httptest 捕获请求，不修改 12 的服务或旧脚本。

## 7. 文档决策记录

| 日期 | 责任与状态 | 修订 |
|---|---|---|
| 2026-10-20 | 主会话，待用户 review | 对齐 00 v0.9：消除白名单与 Extra 的矛盾、保留 messages 结构；补齐 Done／错误／Close；明确内核与适配器分包、bootstrap 单向装配、仓储增量、Key 首次创建与会话接缝；九段 L2 与顺序同步。未写实现，未宣称 B3 验收 |
