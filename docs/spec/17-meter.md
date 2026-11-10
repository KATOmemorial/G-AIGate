# 17-meter（L1）

> 状态：B6施工候选（2026-11-10），待用户review，未冻结／实现。已交付B5基线main@c7837dc；本候选以[00 §6.12](00-总纲.md)为跨模块权威候选，01仍v0.4。
> 影响10／11／13／15／16／17／18／19；不写代码、不运行迁移或服务。§7及原B5移交冻结历史保留，新候选只有用户确认后才替代相应未来消费方式。

## 1. 模块边界

计量、客户报价、只读配额预检、耐久客户明细、幂等Redis投影、月账单及只读API。只对最终成功attempt收费；不是供应商成本台账，不把上游失败消费转嫁给客户，不承诺供应商exactly-once。渠道／重试／SSE及HTTP取消仍属原模块，Key反馈／指标／页面／配置CRUD仍后置。

一个request_id最多一条终态客户明细，有opener的失败／取消留零收费诊断行，未出站／认证／预检拒绝不制造账。前一失败再成功只计最后一次成功usage，不能每attempt重复记录用户请求。原B1“流式逐分片累计usage”修正为累计估算字符、权威累计usage快照只替换不相加。

## 2. 三个L2与交付顺序

| L2 | 完成内容 | 生产启用 |
|---|---|---|
| [17.1 Token采集](17-meter/17.1-token-accounting.md) | 公共类型／纯Collector、usage优先与确定性字符代理 | 不接生产 |
| [17.2 配额与结算](17-meter/17.2-quota-settlement.md) | 新迁移／幂等SQL明细及outbox、SQL权威预检、六计数Lua投影与worker | 库及真实机制验证，不启用半套生产收费 |
| [17.3 计价与账单](17-meter/17.3-pricing-billing.md) | 当前客户报价／整数计价、真实装配替换AllowAll、账单／API／B6检查点 | 同时启用真实Guard／Recorder／workers |

17.1／17.2各M1后确认M2，M2最终回归／独审；17.3分M1库、M2装配、M3全链检查点逐段确认。17.2仍承担Guard实现，#22实际wiring验收前移交17.3收尾，不能只建服务忘记装配，也不能未交价格就先把实际用户写成零元。

## 3. 关键取舍（均待确认）

| 项目 | 本候选 | 成本／限制 |
|---|---|---|
| 收费对象 | 最后成功attempt，失败／取消／写失败0收费 | 不反映供应商全部损耗 |
| 缺usage | canonical输入JSON及可转发输出字符代理，默认4字符/token | 工具／多模态没有准确率承诺，estimated可见 |
| 价格 | 公开别名客户价，成功tokens两方向金额合计一次ceil | B5 cost_first上游排序不是客户价；缺价在首次Provider前503 |
| 账务权威 | 同步幂等SQL明细＋事务outbox，不走丢弃队列 | 多SQL成本；HTTP写出与提交仍非原子，最终提交失败必须事故告警 |
| 配额 | SQL单快照查权威已用、只读不预扣 | 并发已获准可有限超额，不承诺硬零超额 |
| Redis | 六计数＋request_id标记一次原子投影、pending可重放 | 单实例Redis，不支持跨slot Cluster；丢失done投影后的历史重建后置 |
| 分组 | 认证GroupID写入明细，账单按user/group/month | 需要增量迁移；不能用当前users.group_id重写历史 |
| 账单 | 明细实时聚合API＋有延迟bills物化 | 静默重建后才断言完全一致，不能拿旧物化值冒充实时 |

旧UsageRepo.Record无返回值、OnDrop／drain保留，用于原非财务用途；B6正式财务只走新SettlementRepo，禁止双写。#18在17.2唯一键与相同载荷重试落地前仍open。为了账务取舍保留反例：SQL提交前崩溃可漏最终明细，已提交后quota_outbox可恢复；“成功HTTP响应永不漏账”不是本候选能承诺的分布式原子性。

## 4. 配额、API与权限

三身份key→user→group，各日／月，token/cost独立限额0或缺配置不限，>=正限额拒绝，soft_ratio告警不拒。UTC周期按CompletedAt固定，SQL与Redis不在重试时换月；SQL故障500，不能当不限。quota_race验收将并发窗口与明细提交后新增拒单分开，不要求100个同屏障读旧值时必有拒单。

GET /api/v1/user/quota及/user/usage是本人读取；GET /api/v1/billing/summary默认本人，跨用户／group及scope=all须admin。Bearer标准Principal起步，19.1接session，全部no-store。时间域当前及前23个月、JSON分页默认20上限100、CSV至多1000聚合行且超限零部分输出；计数／金额／ID以十进制字符串给前端。详细参数与service层IDOR约束见17.3，不开放公网写价／改额度／重置计数。

## 5. 依赖、生命周期与移交

meter→store／access Principal／provider／middleware，router→meter为实际收费消费；access不import meter，meter不import router／proxy／bootstrap。错误哨兵由access声明供入口与meter同值消费，SQL错误保留cause／安全固定文本，不向客户端泄露价格库／凭据。完整类型见00，不在这里维护第二份签名。

HTTP及在途handler→health stop/join→meter账单／投影worker stop/join→旧usage drain→Redis→MySQL。pending任务落SQL可留待重启，不能留worker或关闭依赖后晚到写。17.2需证明事务／Rollback真网络预算，不能把库WithContext或CLI私有适配当已验证。

#15字段消费／#17异步队列差异／#18幂等／#22正式Guard／#24复杂请求估算均纳入候选但不关闭；#19限流、#20部署fail-fast、#23会话、#25／#32观测展示及#29 Key反馈不被扩大实现。数据迁移要求旧usage_records／bills为空，非空实例停下另定方案；没有改旧000001或自动清数据授权。

## 6. B6验收与review门槛

完整自有HTTP／MySQL／Redis流程，正常无注入故障时每个有opener终态一行、失败0收费、final成功计量与整数价正确、等待outbox清空后六计数增量一致、静默月重建bills=sum明细；100并发弹性窗口及提交后新请求429、软告警、日月边界／身份换组、SQL未知提交／Redis回包丢失／进程恢复、真实取消／SIGTERM与原M0／B4／B5均有机制证据。故障注入区分提交前事故与已提交可恢复任务，不用零FAIL证明所有故障下绝不漏账。

真实SQL各轮-p 1 -count=1、真实Redis多client／多进程及全部新增并发race、全仓回归／两类lint、quota_race.sh语法／正负向；保护原修改／私人材料／旧证据、恢复本轮自有资源。B5交付历史保持，候选及review清单完成不代表B6代码或验收。

## 7. B3 消费接缝（2026-10-20，已获用户确认并冻结）

13.2 的 ChatRequest.Messages 为原始 JSON 对象列表，不能假设 content 恒为 string；17.1 撰写时明确文本／工具／多模态的估算范围与不支持的处理，不为透传结构承诺准确 token 数。真实 usage 仍优先，缺 usage 的成功响应才进入估算；中断与部分写的结算语义由 B6 专门冻结，不把失败写出帧当已交付回调。

15.1 的同步 onChunk 只回调成功转发的 Chunk／UsageDone，不回调 Done／Error；使用结果 Usage 时注意副本与零值。B3 AllowAll 预检尚未实现额度语义，17.2 必须在装配处替换并证明拒单路径；移交见 02，既有 #15／#17 的字段与异步落账消费约定仍待本模块折入。

冻结登记：2026-10-20 用户在主会话确认 B3 文档，本节对应增量已批准；不扩展为对其他待细化内容的整体批准，不改写已交付历史，新增实现仍由指定 L2 负责。

## B5向B6移交（2026-10-30，已冻结）

16.4先交正式路由／回退，但B5不结算、不落usage_records、不替换AllowAll，不直接import未交付meter包。一个逻辑请求可能有多个未提交失败attempt，上游仍可能已消费；B6须明确哪些实际usage／部分失败应结算、最终用户请求与attempt如何关联，不能将未提交当供应商未计算，也不能把每个attempt当独立用户请求重复扣费。16.2的cost_first仅按UpstreamModel的等量输入／输出CNY单价做代理排序，缺价后排不替代17.3的无价格／整数计价策略；#17／#22／#24／#32相关原owner保留。本节已冻结，不因此增表或宣告计量已实现。

冻结登记：2026-10-30，用户在主会话对8666eb4候选回复“我确定”，本节随00 v0.11／16-L1／四份L2生效；正文要求沿已确认候选，不代表代码实现／验收。


## 8. B6候选修订记录（2026-11-10）

用户要求开始下一批文档准备。主会话核对c7837dc及实际UsageRepo／QuotaCounter／PriceRepo／DDL后整理00 §6.12与三L2。上面旧冻结移交中的UsageSink薄包装保持旧API语义，本候选改为财务链路不使用该丢弃队列；旧匿名Settle与缺少group／唯一请求键的模型不能直接支撑本次保证。此为待用户review的显式变更，不追改B2验收，也不视为已经获得迁移或实现授权。
