# 16-router（L1）

> 状态：已冻结（2026-10-30用户确认8666eb4候选，见§10）；00 v0.11 §6.11为B5施工依据，原B3／B4接缝及交付历史保留。层：第1层数据面；批次：B5。尚未实现／验收。
> 上游：[00-总纲](00-总纲.md) §5.2／§6.5／§6.11；工程基线：[01-工程规范](01-工程规范.md)。B4已于main@f3529d0完成交付，B5尚无实现／验收。

## 1. 模块边界

模型别名→候选规划→策略排序→实际许可→逐候选转发／结果反馈。保留健康与breaker两套状态机，复用14.2 EndpointPicker和15 Forwarder，不重写上游协议或流式机制。

B5不实现配额／结算／账单或Prometheus／OTel，不开放客户自定义回退链；原AllowAll明确待17.2替换。本轮文档已获用户确认，后续逐份开发／独审，整体检查点后才交付M1后半；主会话不写实现。

## 2. 契约与取舍

跨模块契约以[00 §6.11](00-总纲.md)为唯一权威。相对旧§6.5的主要变更：

| 接缝 | 已确认方案 | 理由 |
|---|---|---|
| Plan | 无Key的渠道／映射快照，按需Pick Endpoint | 不为未尝试渠道解密或消耗Key SWRR位置 |
| Breaker | 只读Inspect、Acquire许可、Complete(success/failure/neutral) | 规划不占半开，许可去重、lease及epoch隔离晚到结果 |
| 参数与预算 | Route 2s、State 500ms、最多3真实attempt；整体沿proxy Overall默认300s且不刷新 | 防候选数倍增请求时长；更早父期限始终有效 |
| 排序 | 非链式按priority分层；weighted不放回，cost按映射单价代理，latency按本地成功完整attempt近期p99 | 明确主备优先级与现有数据来源，不用不存在的价格列或展示统计 |
| 反馈 | breaker／health独立映射、共用一次有界清理ctx | 错误原因不能覆盖Kind；请求错误／取消／写失败中性释放 |
| 后续批次 | B5只交编排、安全摘要及本地延迟；B6／B7正式结算与观测后接 | 不import未交付包、不用桩冒充计费或指标完成 |

provider.Endpoint、HealthRegistry、EndpointPicker、Provider、proxy及access入口公开签名不变。router使用access身份／错误消费类型，不产生access→router反向依赖；bootstrap只可保留无选择算法的装配适配与AllowAll，16.4移除临时首健康／单次执行逻辑。

## 3. 核心语义

- 对外模型必须是已配置别名，无递归。fallback_chain严格default→fallback_ids，缺失／禁用／无mapping过滤；其他策略取全部可服务渠道。未知模型404、没有候选503、真实配置／基础设施错误保留。
- 规划最多64个去重初始候选，只读健康与breaker。实际尝试重检健康、Acquire、Pick一次；映射已变则中性释放并跳过，不用旧计划强改Endpoint。配置60s缓存／跨存储竞态取舍保留。
- weighted按正配置权重生成组内不重复排列，渠道概率与Key SWRR分离。cost_first按UpstreamModel的CNY输入＋输出单价精确比较、缺价后排，非实际计费。latency_first每实例／每channel和公开alias的成功完整耗时，60s／最近256样本／最少5个，最多4096键；冷样本后排、同值ID确定。
- breaker以Redis TIME统一毫秒时钟，原60s窗口／20样本／50%／open30s。只读Inspect不会转态；实际Acquire全局单probe，closed也发唯一permit；Complete幂等、旧epoch无权改新状态，中性释放、lease恢复崩溃持有者。三key同slot及策略指纹／更改配置需协调清理的取舍见16.3。
- 每channel至多一个实际opener，最多MaxAttempts，不进行相同渠道换Key或Retry-After等待。未提交、可重试、客户端／逻辑预算仍有效且有候选才切换；已提交一律终止，不补第二个成功响应。完整矩阵见00／16.4。
- 本轮不做Key结果反馈／动态降权（#29），不把后台健康probe当breaker的half-open请求。B6另冻重试上游可能重复消费与计量口径，不承诺供应商exactly-once。

## 4. L2清单与顺序

1. [16.1 模型别名与候选解析](16-router/16.1-model-resolver.md)：声明类型、解析／快照／只读过滤，注入Orderer与Inspector；不生产装配。
2. [16.2 路由策略与本地延迟样本](16-router/16.2-route-strategies.md)：四策略、真实PriceRepo、冷启动／有界样本；16.4才喂生产样本。
3. [16.3 Redis三态熔断与尝试许可](16-router/16.3-circuit-breaker.md)：Lua、lease／epoch／去重及真实Redis多副本验证；不改健康。
4. [16.4 容灾执行器与B5正式装配](16-router/16.4-failover-executor.md)：接入真实网关、移除临时编排、双反馈／预算／真实进程和failover.sh。

16.1用行为测试替身注入尚未实现依赖，不提供生产永远放行的breaker。每份M1后等用户确认，完成本份最终回归／独审／授权交付后才开下一份；16.4分M1循环、M2接线、M3检查点三个停点。

## 5. 风险与批次边界

策略当前数据有限：成本是映射模型的等量token代理，本地p99是近期有限成功样本；不冒称渠道采购成本或全量跨副本P99。breaker需要真实Redis跨client原子证据，epoch／pending不能只在内存实现；Lua并不自动回滚脚本中已执行的写。状态HASH无TTL及策略修改需协调清理是明确运维取舍。

#21／#31的B5接管、#25 Kind／计时、#32数据来源折入本次冻结正文；#29、B6部分计量和B7展示／指标仍保留。跨存储配置TTL与请求间配置变化不升级为分布式事务。相关00／10／11／13／14／15／17／18消费说明同步登记冻结。

## 6. B5检查点与review门槛

自有双渠道mock，验证主故障→备成功→窗口open→跳过主→到期并发最多一probe→成功closed→回主；另证已提交中断／写失败／客户取消不切换和同一writer多attempt。隔离health阈值／probe，防健康遮蔽breaker。默认语义单测与显式加速fixture分开；真实Redis／多进程、Linux SIGTERM、全部M0／B4回归，脚本只新增failover.sh、不修改旧资产。

用户已按00契约→L1取舍→L2细节确认候选，见[review与冻结记录](../reviews/B5-施工文档-review清单.md)。本轮登记冻结并整理16.1开发prompt，新开发会话先做M1；后续按停点确认，当前不表示实现或B5验收完成。

## 7. B3 消费接缝（2026-10-20，已获用户确认并冻结）

00 §6.5 的 router.Executor 仍接收 Plan；access.ChatExecutor 仅是入口需求，不能声称同签名。B5 由装配适配器先 Route 后 Execute，并移除 B3 bootstrap 的直连选择。Forwarder 接受 opener，而非先建立 Stream 再传入；TTFB 单段覆盖连接／响应头／首个可转发结果，FirstByteMs 不再加 ChatStream 耗时。WroteHeader 只排除已提交切换，不代表 false 就该重试；客户端取消及不可重试上游状态仍单独判断。

结果类别新增 upstream_error／write_error，参见 15.1，不能假设 OverallTimeout 必已写头。用 middleware.WithChannelID 同步本请求摘要，在候选切换后完成日志能反映最终实际渠道；不要回写全局或跨请求共享 context。临时路径接管与观测口径登记 02，正式 L2 撰写时消费。

冻结登记：2026-10-20 用户在主会话确认 B3 文档，本节对应增量已批准；不扩展为对其他待细化内容的整体批准，不改写已交付历史，新增实现仍由指定 L2 负责。

## 8. B4接管消费（2026-10-22，已冻结）

00 v0.10 §6.10／14.1～14.3先交付配置SWRR与Redis健康；B4 bootstrap仅按priority／id预先过滤健康后选一个，不重试，不实现breaker。16.1／16.4接管时移除临时选择／健康接线，继续用HealthRegistry及EndpointPicker（明确*store.Channel）；别名引用缺失渠道须过滤，候选全空503，不因保留引用而panic。

健康结果成功true、中断／TTFB／Overall为false；上游错误除HTTP400／404／422忽略外计false，客户取消／下游WriteError忽略。先按Kind、仅上游错误再看状态码，不从原因链Canceled改判；正式执行器双喂breaker时另冻各自分类，不把两个状态机合并。14.2无Key失败反馈／动态降权，Endpoint无KeyID，未来另审。

14.3不生产错误率／p99展示缓存，latency_first的数据来源、空样本／全零时确定性排序须16.2开工前细化，不以probe单点值冒称p99。18.1仍为被动helper，不授权metrics反向import业务包。本增量已冻结，不改此前B3冻结记录；尚无B4／B5实现。

冻结登记：2026-10-22，用户在候选提交4eb8192及review清单交付后回复“请你开始工作”，本节随00 v0.10／B4文档确认生效；只登记既有候选效力，不新增范围，不代表实现／验收。

## 9. B5候选决策记录（2026-10-30）

B4已独审并授权合并3335f33／收尾f3529d0；此前§8“尚无B4／B5实现”为2026-10-22阶段记录，B4现已交付，B5仍未实现。主会话按用户“请你开始工作”完成B4登记和本候选；原批准的B3/B4消费接缝保留，新增Plan／permit／统计／预算语义须用户review，不把本次开始写文档理解成候选已批准。

## 10. 用户确认与冻结登记（2026-10-30）

2026-10-30，用户在主会话接收候选8666eb4及review清单后回复“我确定”。据此确认00 v0.11 §6.11、16-L1、16.1～16.4及相关消费说明为已冻结开工依据；只登记原候选生效，不新增字段、算法、参数或验收要求。冻结不代表已有代码／自测／独审／合并；16.1先M1，后续阶段依前置及用户确认停点推进。01 v0.4与B2～B4交付历史保持。
