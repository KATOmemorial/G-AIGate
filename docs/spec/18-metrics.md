# 18-metrics（L1）

> 状态：B1 评审中 | 层：第 2 层（横切，被 13~17 打点调用，不依赖任何业务包）| 批次：B7
> 上游：[00-总纲](00-总纲.md) §7.3 / §8 | 工程基线：[01-工程规范](01-工程规范.md)

---

## 1. 模块边界

**做什么**：指标注册与打点 helper（统一命名与维度）；OTel span 体系；RequestID ↔ trace_id 关联。

**不做什么**：指标存储与面板（20.3 Grafana）；任何业务判断。本模块是纯被动的观测基础设施。

## 2. 指标清单（命名在 18.1 冻结，维度白名单见 00 §7.3）

| 指标 | 类型 | 维度 | 用途 |
|---|---|---|---|
| `gaigate_requests_total` | counter | channel, provider, model, status_class | QPS / 错误率 |
| `gaigate_request_duration_seconds` | histogram | channel, model, status_class | 网关端到端延迟（含 P99） |
| `gaigate_upstream_ttfb_seconds` | histogram | channel, model | 首字节延迟（容灾切换判定的观测面） |
| `gaigate_tokens_total` | counter | direction, channel, model | Token 用量 |
| `gaigate_cost_total_microyuan` | counter | channel, model | 成本 |
| `gaigate_breaker_state` | gauge | channel | 0=closed 1=open 2=half-open |
| `gaigate_channel_health` | gauge | channel | 1=可用 0=禁用 |
| `gaigate_quota_events_total` | counter | target_type, event | 软阈值/硬超限事件 |
| `gaigate_usage_drop_total` | counter | — | 落账队列丢弃（11 风险联动） |
| `gaigate_failover_total` | counter | from_channel, to_channel | 容灾切换次数（演示核心指标） |

`status_class` 为有限集：`2xx / 4xx / 5xx / canceled / interrupted`。**user_id 不进任何 label**（高基数），用户维度查 `usage_records`。

## 3. 关键设计决策

1. **Prometheus pull 模式，`/metrics` 独立内部端口**，不随业务端口暴露。
2. **span 树与管道同构**：`request → route → upstream.attempt[i]（每次尝试一个，失败原因打属性）→ stream → settle`——容灾过程在 trace 里逐跳可见，配合 `gaigate_failover_total` 定位"哪次请求为什么切了渠道"。
3. **首版全采样**（内部规模 OK），采样开关留配置项。

## 4. L2 清单（与 00 §4.1 预排一致，无调整）

- **18.1 Prometheus 指标**：注册、打点 helper、维度常量；B7 前即可先行被各模块调用。
- **18.2 OTel 链路**：span 体系、RequestID ↔ trace_id 双向关联（日志字段带 trace_id）。

## 5. 主要风险

- histogram bucket 选型（0.05s ~ 30s 对数分布）不当会毁掉 P99 观测——18.1 冻结 buckets 并说明依据。

## 6. 验收要点

B7 前置：`/metrics` 文本可查全部指标；一次容灾请求的 trace 能看到两次 `upstream.attempt`。B7 贡献：Grafana 面板出图（20.3）。

## 7. B3 观测输入（2026-10-20，已获用户确认并冻结）

15.1／15.2 的 FirstByteMs 从一次尝试进入起算，覆盖连接、响应头和首个完整可转发载荷；非流式为完整 JSON 就绪。它是应用层口径，未就绪为 -1，不是 TCP 首字节或客户端接收时刻；18.1 不得再加一次 ChatStream 耗时或把 -1 写入成功延迟样本。正式命名／指标说明在 18.1 冻结，不改本轮之外的 label 契约。

ForwardResult 的结果类别不能替代 HTTP 提交状态：HTTP 200 可伴随后续中断，需独立的失败／中断观测；WroteHeader=false 也不保证应当切换。移交记录见 02，B3 不提前实现 Prometheus／OTel。

冻结登记：2026-10-20 用户在主会话确认 B3 文档，本节对应增量已批准；不扩展为对其他待细化内容的整体批准，不改写已交付历史，新增实现仍由指定 L2 负责。

## 8. B4观测移交（2026-10-22，已冻结）

14.3冻结范围仅镜像health_status／last_checked_at，不用单次probe latency伪造latency_p99_cache或error_rate_cache。未来统计样本、窗口、冷启动及展示列生产由14后续轮／16.2与18.1共同冻结，现阶段台账#32 open。18仍是被动观测helper，不因此获得import store／channel或直接写库的权限；聚合／装配owner须另行明确。

已有proxy FirstByteMs／Kind口径不变，probe总耗时不混进业务TTFB／p99样本；B4暂不实现Prometheus／OTel或页面。文档已冻结，尚未实现。

冻结登记：2026-10-22，用户在候选提交4eb8192及review清单交付后回复“请你开始工作”，本节随00 v0.10／B4文档确认生效；只登记既有候选效力，不新增范围，不代表实现／验收。

## B5观测与延迟统计消费（2026-10-30候选，待review）

16.2拟自持本实例按(channelID,公开alias)的近期成功完整attempt耗时样本（60s／最多256／至少5／4096键），供latency_first；16.4同步喂样本。它不是proxy FirstByteMs、不是probe、不是Prometheus histogram或跨副本全量P99，不读取MySQL两展示列。#32中展示聚合／error_rate_cache／latency_p99_cache生产仍后置，未来另冻owner，不赋予18反向import router／channel／store权限。B5仅安全attempt／请求摘要与具名TODO，无Prometheus／OTel假实现；18后续按单逻辑请求及每实际attempt区分计数／耗时，沿原标签白名单，不把用户或Key入label。候选以00 §6.11及16.2／16.4为准，待用户review。
