# 11-store（L1）

> 状态：B1 评审中 | 层：第 0 层基座 | 批次：B2
> 上游：[00-总纲](00-总纲.md) §6.3 / §7 | 工程基线：[01-工程规范](01-工程规范.md)

---

## 1. 模块边界

**做什么**：MySQL schema 与版本化迁移；全部表的访问层（users / groups / api_keys / channels / channel_models / channel_keys / model_aliases / model_prices / usage_records / quotas / bills）；Redis 状态原语（配额已用计数、健康状态存取、限流计数预留）。

**不做什么**：不解密 `provider_key_enc`（返回密文，解密归 14.2）；不做业务判断（健康状态机在 14.3，熔断在 16.3，配额语义在 17.2）；不暴露任何 HTTP。

**缓存策略（本模块重要决策）**：渠道、别名、价格等配置类数据走进程内 cache-aside 缓存——TTL 60s + 管理变更时主动失效本副本。理由：router 每请求都要读配置，不能打 DB；多副本间不做失效广播（pub/sub 复杂度不值），60s TTL 收敛可接受。这是**文档化的已知取舍**：渠道删除后最长 60s 内旧副本仍可能被选中，由请求失败→健康禁用自然兜底。

## 2. 接口

- 跨模块契约见 00 §6.3（`ChannelRepo` / `ModelRepo`）。
- 模块内接口（各 L2 冻结清单）：`UserRepo`、`APIKeyRepo`、`UsageRepo`、`QuotaRepo`、`BillRepo`、`PriceRepo`、`QuotaCounter`（Redis HINCRBY）、`HealthState`（HASH 读写）。
- 供 14.3 使用的 `HealthState` 读写与供 17.2 使用的 `QuotaCounter` 是 Redis 契约（key 命名见 00 §7.2）的 Go 封装，语义在对应消费模块的 L2 冻结，本模块只提供原子原语。

## 3. 关键设计决策

1. **golang-migrate 版本化 SQL 迁移，禁用 GORM AutoMigrate**：迁移可 code review、可回滚、多副本升级时 schema 状态明确。
2. **usage_records 只追加、不更新**：账目不可变是"账单 = 明细求和"对账的基础；修正走补偿记录（首版不实现，预留口径）。
3. **落账异步化**：`UsageRepo.Record` 由 meter 经内存队列调用，队列满或进程崩溃时打 `gaigate_usage_drop_total` 计数并丢弃——已知缺陷，文档化，修复方向（本地 WAL）后置。面试可讲"账务系统的丢失窗口"。
4. **配置读缓存 + 账单写异步**：读写路径分开优化，热冷分离呼应 00 §7 的 Redis/MySQL 分工。

## 4. L2 清单（与 00 §4.1 预排一致，无调整）

- **11.1 MySQL schema 与迁移**：全部 DDL、索引、seed 数据（admin 用户、指向 mock 的示例渠道、示例价格）。
- **11.2 渠道/Key/别名/价格访问层**（含 cache-aside 缓存与主动失效）。
- **11.3 计量/配额/账单访问层**（含异步落账队列）。
- **11.4 Redis 状态原语**（miniredis 单测；限流计数仅预留接口）。

## 5. 主要风险

- 缓存不一致窗口（见 §1，60s 兜底）。
- 异步队列的丢失窗口（见 §3.3，打点可观测）。
- 迁移与 GORM 模型漂移——验收要求 seed 数据可被访问层完整读出，作为 schema 与代码一致性的烟雾测试。

## 6. 验收要点

B2 检查点贡献：`docker compose up mysql redis` 后 `go test ./internal/store/...` 全绿；seed 后 `ListActiveByModel("gpt-4o-demo")` 返回示例渠道。
