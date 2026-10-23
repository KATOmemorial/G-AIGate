# 14-channel（L1）

> 状态：已冻结；14.1已验收、合并并推送main（2026-10-23，见§8），14.2／14.3未实现，B4检查点未完成 | 层：第1层数据面 | 批次：B4
> 上游：[00-总纲](00-总纲.md) §6.4 / §7.2 | 工程基线：[01-工程规范](01-工程规范.md)
> 本模块是设计文档 §4.2（渠道管理）的落地，是"上游挂了怎么办"答案的前半段（持续可用性）。

---

## 1. 模块边界

**做什么**：渠道／映射／渠道Key管理、手动最小chat测试、Key平滑加权轮询（SWRR）、端点装配、Redis共享健康状态与定时探活。复用既有secret／仓储／Provider／proxy。B3／M0已验收，本批从14.1开始。

**不做什么**：熔断（16.3——那是请求级滑动窗口三态，与本模块的渠道级持续可用性判定是两套独立机制，窗口、阈值、恢复语义均不同，见 00 §5.2）；路由选择（16）。

## 2. 接口

- 跨模块契约：00 v0.10 §6.4／§6.10；EndpointPicker渠道参数明确为`*store.Channel`，Endpoint不增加KeyID。
- HTTP完整前缀`/api/v1/admin/channels`，含明细、Key子资源及手动测试，详见14.1。B4注入既有Bearer鉴权＋数据库真实admin角色；普通用户Key不可管理。19.1接管cookie session／CSRF，19.2交付页面；不开免鉴权管理口或固定管理token。
- channel可依赖store／secret／provider及既有middleware，不import access／bootstrap／router／proxy／provider/openai；具体Provider及身份函数由cmd注入。cmd／bootstrap可依赖channel，不形成环。
- 健康状态机参数（默认值，全部可配，语义在 14.3 冻结）：连续失败阈值 5 → 禁用；禁用冷却 60s；冷却结束后半开——探活成功才恢复；探活周期 30s。

## 3. 关键设计决策

1. **健康与熔断分离**（本模块最重要的可讲点）：健康状态机回答"这个渠道还能不能长期信任"（连续失败/探活信号 → 禁用是**排他性**的，禁用后不进候选）；熔断回答"这个渠道最近一分钟错误率高不高"（比例信号 → 三态，半开可探测恢复）。同一请求结果同时喂给两者（`ReportRequestResult` + `Breaker.OnResult`），但判定独立。
2. **多Key SWRR仅按配置权重**：active且weight>0的Key参与，每个picker实例自持序列，不承诺分布式严格比例。原草案“单Key失败临时降权”已确认后置：现有Endpoint无KeyID、结果只有channelID，不能把渠道失败臆测为特定Key失败；未来反馈契约另审，见台账#29。
3. **状态存Redis**（key见00 §7.2 `gaigate:health:{channelID}`）：多副本共享健康真相；B4仅异步镜像MySQL health_status／last_checked_at。error_rate_cache／latency_p99_cache的统计生产后置，健康放行不读展示缓存。
4. **最小非流式chat探活**：max_tokens=1、单消息、默认5s总预算，复用Provider.Chat；后台按model升序首映射及id升序首启用Key，手动测试选指定映射。不消耗业务SWRR位置，只证明该配置路径，不宣称所有Key都健康。
5. **管理事务**：PUT完整管理标量＋全量models，同一事务替换；Key单独增／改weight与status／删。写成功失效本副本共享ConfigCache；其他副本仍按60s TTL，不增加广播。
6. **健康转移原子化**：现有Load／Save不保证读改写原子性。14.3增store.VersionedHealthState（版本读＋CAS），同一HASH新增version，不改旧HealthState／HealthSnapshot。版本不匹配的晚到探活不得覆盖新状态，多副本可重复探测但只有匹配版本结果提交。
7. **禁用到期不自动放行**：DisabledUntil=0允许；非0包括已过冷却均排除，到期只允许后台探活，成功才恢复。普通在途成功不解禁；Redis错误／坏状态不放行。
8. **展示分离**：MySQL仅异步镜像health_status／last_checked_at；不以单次探活冒充error_rate_cache／latency_p99_cache，统计生产由14后续轮／16.2与18.1再冻结，18只提供被动helper，不引入metrics→业务反向依赖。14.3新增admin实时health端点供检查点使用，不依赖尚未交付页面。

## 4. L2 清单（与 00 §4.1 预排一致，无调整）

- [14.1 渠道CRUD与连通性测试](14-channel/14.1-channel-admin-api.md)：M1管理／仓储增量；M2 HTTP鉴权／手动测试／装配。故障由自有mock控制API设置，不扩展adapter透传任意测试header。
- [14.2 加权轮询与端点装配](14-channel/14.2-endpoint-picker.md)：M1 SWRR／快照／解密映射；M2 bootstrap真实接管与M0回归。
- [14.3 探活与健康状态机](14-channel/14.3-health-registry.md)：M1原子原语／状态机；M2任务及网关接管；M3真实Redis／进程和B4检查点。

三份文档已确认冻结，按14.1→14.2→14.3依次开发／独审／授权交付；每个里程碑自测后等用户确认。14.2只替换首Key／端点装配，14.3增加健康过滤及结果上报，仍按priority／id选首可用渠道、失败不重试；B5才移除临时编排并接入正式路由／回退。台账#21按部分接管记录，不在B4整项关闭。

## 5. 主要风险

- 探活流量成本：每渠道每 30s 一个 1-token 请求，内部规模可忽略；探活失败到禁用生效之间存在窗口，在途请求自然失败并由熔断兜底——两套机制的衔接点，面试必问。
- 解密每请求一次：首版直接解，缓存解密结果（TTL = 渠道配置版本）作为后置优化，先保证正确。

## 6. 验收要点

B4检查点：自有mock切downtime→有效失败信号达到阈值→实时available=false→冷却已到仍false→normal后探活成功→available=true→网关恢复。脚本新增`test/e2e/health_lifecycle.sh`，14.3自有fixture负责隔离资源及故障；不改旧脚本或共享mock，不依赖admin页面。B3 M0持续回归；正式容灾／配额／部署不冒称已交付。

沿用probe-interval=30s、failure-threshold=5、disable-duration=60s；14.1增probe-timeout=5s，14.3增probe-concurrency=4、state-timeout=500ms。配置结构／默认值／校验／环境／样例由对应L2交付，无DDL／seed／依赖升级。后台关闭顺序为HTTP停止并处理在途→探活取消等待→usage drain→Redis→MySQL，不改HTTP Shutdown协议。

## 7. 移交与已知取舍

#9渠道列表、#12映射与Key管理折入14.1；#11仅记录ModelRepo.Update全量覆盖风险，别名／价格编辑API归19.2，不扩张本L2。#21正式路由留16.1／16.4。其余已确认移交见02 #28～#32，尚未实现／关闭。

跨副本配置旧值窗口仍60s；SWRR无全局序列；多副本探活允许重复出站；Redis不可达时健康不放行。管理员可指定私网／loopback上游以支持mock与内网，不承诺公网SSRF隔离；URL禁止userinfo／query／fragment，错误不回显URL／凭据。

## 8. 决策记录

2026-10-22，主会话依据main@13048ee及B3验收补齐B4候选文档。单Key反馈后置、admin Bearer过渡、管理事务、版本化健康原语和实际检查点须用户集中review；当前未冻结、未实现，不改B2／B3验收历史。

同日，候选提交4eb8192及review清单交付后，用户回复“请你开始工作”，确认推进冻结与14.1开发prompt。本L1及三份L2随00 v0.10登记冻结，正文要求沿候选不变；14.1新实现会话先交M1，其余阶段按前置与停点，暂无实现／独审／验收。

2026-10-23，14.1完整范围8ebca98..bbbb1f0经独立复审“可合并”，原父取消隐式HTTP200的P2已关闭，未发现新增问题；限定／Linux race各115顶层／344子用例、全仓310顶层／1363子用例通过，零FAIL／SKIP／race报告，lint与完整diff检查通过。用户回复“我授权”后本地合并fa2da82ad5fc70a6d30ae5318211fc955f861f17，合并树与已审交付完全一致；详见[14.1 §11.13](14-channel/14.1-channel-admin-api.md)。14.1管理、手动最小探测及共享缓存装配已验收；14.2 SWRR、14.3健康／任务／实时health及B4完整检查点均未交付。#9关闭，#12仅渠道mapping部分完成、#28仅Bearer admin过渡完成，其余未来接管不关闭。本轮只登记本地交付，未push、删除分支或推进14.2。

同日，本地交付后用户再次回复“我授权”，普通推送main至b927468并经实时ls-remote核对一致；包含14.1合并fa2da82、验收ae0a1bf及主会话已提交的14.2准备文档。推送收尾文档随后单独提交并普通推送，最终tip见14.1 §11.14及交付汇报。已审实现保持不变，未启动测试服务或实现14.2／14.3，B4整体检查点仍未完成；本地l2/14.1分支和保护文件保留。
