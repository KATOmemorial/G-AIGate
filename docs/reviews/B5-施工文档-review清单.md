# B5施工文档review清单

> 2026-10-30主会话整理；事实基线main@f3529d0，B4收尾治理e7ff069。**B5为v0.11候选，待用户review，未冻结／授权实现。** 本清单是阅读和决策入口，不是独立批准报告。

## 1. 本轮已完成的事实登记

14.3已由独立会话审查a3d6121可合并，用户授权后合并3335f33、收尾f3529d0推送，交付会话实时核验远端。本轮主会话只读核对当前main／本地origin/main及delivery-final.json、已审源码无差异，没有再次联网或复跑测试。B4已登记完成，#30／#33关闭；#21／#31只完成B4部分，B5正式接管仍open。

原14.3 HTTP P2及后续CLI预算P2保持关闭；独立391顶层／1621子用例、限定及Linux race152／472、两类lint和同源码B4七组证据保留。原失败／阶段报告未重写，不把文档候选当成新代码通过。

## 2. 阅读顺序

1. [00 §6.11候选](../spec/00-总纲.md#611-b5路由熔断容灾施工契约v011候选待用户review)：跨模块类型、三key、策略来源、预算与重试矩阵；§6.5／§7.2旧基线批准前仍有效。
2. [16-L1](../spec/16-router.md)：边界、取舍、四份L2及施工顺序。
3. [16.1候选解析](../spec/16-router/16.1-model-resolver.md)、[16.2策略](../spec/16-router/16.2-route-strategies.md)、[16.3熔断](../spec/16-router/16.3-circuit-breaker.md)、[16.4执行器](../spec/16-router/16.4-failover-executor.md)：各§3契约、§5验收、§8里程碑。
4. [02台账](../spec/02-遗留事项台账.md) #21／#25／#29／#31／#32及10／11／13／14／15／17／18 L1页末B5消费说明，全部新增部分均为候选；01 v0.4不改。

## 3. 需要用户确认的候选取舍

| 项 | 本候选选择 | 为什么需要明确 |
|---|---|---|
| Plan内容 | 值快照Channel＋UpstreamModel，不预存Endpoint／明文Key，最多64初始唯一候选 | 只在实际尝试时Pick，避免未使用Key消耗轮询位置和扩大密钥持有范围；与旧§6.5字段不同 |
| 别名／priority | 非链式全模型候选按priority分层再排序；fallback_chain严格default→fallback，不自动扩链 | 决定高优先级与低成本／低延迟冲突时谁先；缺失引用过滤，坏配置／SQL不当503 |
| 策略继承 | HTTP只传Auto，别名非空strategy优先全局默认 | DDL／seed已有的weighted仍是显式值；改全局默认不会覆盖它，不用“等于默认值”猜测未设置 |
| 熔断接口 | Inspect只读、Acquire带lease、Complete带permit及success/failure/neutral | 旧Allow二次调用会抢名额，无token无法去重、隔离旧结果或中性释放 |
| 半开与崩溃 | Redis TIME、全局一个probe、epoch隔离、lease失效可重领 | 不用本机时钟或进程mutex自证跨副本原子，不让取消／写失败当成功恢复 |
| Redis运维 | HASH状态＋events／pending ZSET，同slot；HASH不TTL，指纹不符fail closed，改策略需协调清理三key | 防状态过早丢失或不同配置副本互相重置；这是需要接受的运维约束，非后台自动修复 |
| 成本排序 | 按映射模型当前CNY输入＋输出单价作等量代理，缺价后排，priority仍优先 | 现有model_prices没有channel_id，不能宣称有不同渠道采购价；不新增DDL或混入计费 |
| 延迟排序 | router自持本实例近期成功完整attempt p99，60s／256／至少5／4096键，未知后排、不主动探索 | MySQL展示列目前没有生产者；不以probe或FirstByteMs冒称p99；全冷按ID，未尝试渠道可持续未知，不承诺自动找全局最优 |
| 总预算／次数 | 整个Route＋执行共享proxy Overall（默认300s），每attempt原TTFB／Overall继承剩余；最多3真实opener、同channel一次 | 不把3个候选变成3份请求总预算；64候选只是规划／遍历上限，最多尝试数独立可配1..16 |
| 回退边界 | 未提交且可重试且ctx有效才切；HTTP400／404／422、401／403、协议错误、取消／写失败停止 | WroteHeader与Kind独立，Retryable不等于“任意上游错误”；头后不补JSON／Done；反馈失败不重试 |
| 双反馈 | 一次共同有界清理ctx，breaker Complete→health Report；各自独立分类 | 保留14.3健康语义及proxy归因，不合并状态机；未实际发起不报健康 |
| 后续批次 | B5不结算／落账／指标／trace，AllowAll留17.2，安全摘要＋具名TODO | 未提交重试可能仍发生供应商消费，B6另冻attempt／逻辑请求结算；#29动态Key降权继续后置 |
| 验收与阶段 | 16.1→16.2→16.3→16.4，M1后确认，16.4再M2／M3；真实Redis／真实进程B5检查点 | miniredis／race不能替代跨副本Lua证据；演示显式隔离health，默认参数与加速fixture分开 |

上述接口为尚未实现router的替代候选，不影响已交付provider／proxy／health公开签名。新增配置按L2明确归属，各自补默认／env／样例／边界；旧e2e脚本不改、新增failover.sh。已有SQL串行、隐私及自证隔离规则继续执行。

## 4. 确认后的工作

依据[01 §9](../spec/01-工程规范.md#9-工作流与角色分工)：“主会话的文档决策由用户review”。用户确认后，主会话才统一登记00 v0.11／16-L1／四L2及消费说明生效，并编写16.1开发prompt；随后另开实现会话，先做M1。未确认前不发实施指令、不改生产代码、不宣告B5开工或已验收。

用户可以直接指出上表需要调整的行；主会话先修改候选再冻结。保留B3／B4接缝历史和原契约基线，候选确认不等于未来代码的独立验收。
