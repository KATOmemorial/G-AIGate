# 20-deploy（L1）

> 状态：B1 评审中 | 层：第 3 层 | 批次：B8
> 上游：[00-总纲](00-总纲.md) §9 | 工程基线：[01-工程规范](01-工程规范.md)
> 目标（用户已确认）：`docker compose up` 能正常启动全链路即可，Helm/Grafana 讲得清、跑得通为度。

---

## 1. 模块边界

**做什么**：docker compose 一键起全链（gateway / mysql / redis / mockprovider / prometheus / grafana，含 seed 与迁移自动执行）；Helm chart（Deployment + HPA + Service + ConfigMap/Secret）；Grafana 面板 JSON 与告警规则。

**不做什么**：云厂商特化（云盘/托管数据库接法仅文档说明）；多环境管理（首版 dev/prod 两套 values）；CI/CD 管线（列为可选）。

## 2. 关键设计决策

1. **mock 常驻 compose**：一键环境天然具备容灾演示能力（配合 `test/e2e/failover.sh`），面试演示零准备。
2. **镜像多阶段构建 + distroless 基底**：体积与攻击面，可讲。
3. **HPA 指标首版用 CPU + RPS**：自定义指标（如按 QPS 的 prometheus-adapter 链路）文档说明方案与代价，不默认部署——"知道怎么扩、知道为什么先不扩"是诚实版答案。
4. **配置热更新的边界讲清楚**：渠道/别名/价格/配额走 DB + 11 的缓存（60s TTL 天然热更，管理界面即时生效本副本）；进程级配置（端口、超时）不热更，滚动重启。哪类配置热更、哪类不热更，是设计文档"配置热更新"目标的准确兑现。

## 3. L2 清单（与 00 §4.1 预排一致，无调整）

- **20.1 docker compose**：全链编排、启动时自动迁移与 seed、`make demo` 一条命令跑通容灾演示。
- **20.2 Helm 与水平扩容**：chart、HPA、扩容验证方法（本地 minikube 起两副本 + 压测观察分流）。
- **20.3 Grafana 面板与告警**：三块面板（网关总览 / 渠道健康与容灾 / 成本与用量），告警规则（熔断持续 open、错误率、软阈值触发）。

## 4. 主要风险

- compose 全链本地资源占用约 2GB——文档标注最低配置。
- Helm 仅 minikube 验证，不承诺生产可用性——范围诚实。

## 5. 验收要点

B8 检查点（项目收官）：干净机器上 `docker compose up -d && make demo`，容灾演示脚本全绿；Grafana 面板出图；`docker compose down -v` 后可完整重建。
