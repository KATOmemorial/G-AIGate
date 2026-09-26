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
