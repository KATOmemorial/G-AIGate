# 15-proxy（L1）

> 状态：已验收；15.1／15.2均已合并推送，正式网关集成随13.3于2026-10-22完成独立审查及授权交付，B3／M0已验收（见13.3 §11.13～§11.14） | 层：第 1 层数据面 | 批次：B3
> 上游：[00-总纲](00-总纲.md) v0.9 已冻结 §3.2／§5.2／§6.1／§6.9 | 工程基线：[01-工程规范](01-工程规范.md)

## 1. 模块边界

负责一次上游尝试的 SSE／JSON 转发、提交状态、同步计量回调、资源关闭、背压、取消和双超时。只依赖 provider 类型，不知道 Principal、候选链、渠道选择或账务策略。

proxy 调用注入的 opener，才能在发起上游请求前建立同一预算；这是执行一次请求的传输机制，不是选择渠道或执行重试。调用方提供已经选定 Endpoint 的闭包。

## 2. 接口与责任

完整类型见 [15.1 §3](15-proxy/15.1-sse-forward.md)：

```go
type StreamOpener func(context.Context) (provider.Stream, error)
type ResponseOpener func(context.Context) (*provider.ChatResponse, *provider.Usage, error)
type Forwarder interface {
    Forward(context.Context, http.ResponseWriter, StreamOpener,
        ForwardOptions, func(provider.StreamEvent)) (ForwardResult, error)
    ForwardNonStream(context.Context, http.ResponseWriter, ResponseOpener,
        ForwardOptions) (ForwardResult, error)
}
```

ForwardResult.WroteHeader 记录实际提交边界；它是上层能否再写 JSON／考虑候选切换的必要条件。false 不等于一定重试，客户端取消、不可重试上游错误仍不能盲目换渠道。任何结果类别都可能在提交前发生，不以类别猜测提交状态。

2026-10-21归因补充（已获用户确认，见15.2 §11.7）：真实HTTP/1.1中Overall写超时可引发请求ctx取消，按00 §6.9第8项／15.2 §3.4确定并锁存Kind，保留父先与不可分辨同时窗口的明确边界；错误链含Canceled不等于最终Kind必须ClientCanceled。只在proxy内部实现，不扩大Provider／middleware或公开接口；已随15.2独立复审及交付完成，见§11.17～§11.18。

proxy 关闭上游并返回错误；HTTP handler 决定未提交时写错误、已提交时 ErrAbortHandler 异常终止。proxy 自身不 recover／panic，不向流内塞错误帧。13.3 对接外层 Recover。

## 3. 关键设计决策

1. **应用层 data 保真**：适配器给出 Raw data，proxy 按 SSE 规则重建 data 行和空行；不把 Chunk 解析再序列化。HTTP 分块、换行风格、注释等不保证逐字节相同。Done 显式转发一次；内容／usage／Done 分别计数，Chunks 不包含后两者。
2. **同一尝试预算**：进入 Forward／ForwardNonStream，在 opener 前计时。TTFB 覆盖建连、响应头及首个合法可转发 data（非流式为完整 JSON）；首个有效载荷就绪后停 TTFB。Overall 含慢客户端 Write／Flush 和正常清理。FirstByteMs 是应用层可转发时刻，未出现为 -1，不是客户端收到时间。
3. **有界链条**：适配器 worker → 固定容量 channel → 同步转发写出；慢写让 channel 写满，停止继续解析上游。不能用每帧启动 goroutine 或无界队列绕开反压。
4. **阻塞写必须退出**：在 commit 前确认底层 writer 的错误 Flush／写 deadline 能力；Overall 写期限和取消后的主动打断覆盖正在阻塞的 Write／Flush。所有 watcher 与 worker 有唯一 owner 并等待退出。生产链不支持能力时，在未提交状态报错，不能静默声称有保证。
5. **非流式直接 Chat**：完整 JSON 有界读取并一次转发，不用 SSE 缓冲伪造非流式。缺 usage 可由未来 meter 估算；错误 JSON、空响应或超限不提交成功头。

## 4. L2 清单与边界

- [15.1 SSE 与非流式转发](15-proxy/15.1-sse-forward.md)：已验收，交付 opener 接口、内容／usage／Done、提交标记、错误分类、Write／短写／Flush 停止、关闭与同步回调。按已获用户确认的 00 §6.9 第7项裁定，标准 WrapWriter.Unwrap 最小透明能力及真实包装链回归也已交付，见15.1 §11.12～§11.13。继承父 context 取消；双预算和阻塞写完整保证仍在 15.2。
- [15.2 背压／取消／双超时](15-proxy/15.2-backpressure-cancel-timeout.md)：已验收，不更改15.1对外签名，交付opener前预算、底层deadline、worker／watcher收敛及真实慢读者验证；原P2返工后独立复审关闭，证据见§11.14～§11.18。

15.1 单独验收不代表 15 模块完整交付；13.3 集成必须等待 15.2。配置沿用 ProxyConfig：TTFBTimeout=10s、OverallTimeout=300s、StreamBuffer=64；channel 容量在 13.2 构造时注入，不另造重复配置。

## 5. 风险与验证边界

B3 以本机 HTTP/1.1 实测；不承诺未经验证的 HTTP/2／TLS／反向代理端到端时序。Gin 声称实现 Flusher 不足以证明底层支持错误 Flush／写期限，需要真实装配链验证。只取消 context 不能打断阻塞写，测试必须包含真实 TCP 不读响应的客户端。

关闭预算需区分取消信号发出与 worker 已退出；用进入屏障、退出通道及 WaitGroup／errgroup 证明，不比较脆弱的全局 goroutine 数。时间用宽容区间，失败时输出具体阶段。

## 6. 验收贡献

正常 8+1+1；缺 Done 的异常流失败；已提交后保留已发送帧而真实异常终止；慢响应头在 TTFB 内退出且未提交；客户端取消后上游收到取消并收敛；慢读者背压有界、Overall 可打断 Write。下游处理错误时仍能写一次正确 JSON，不能把“proxy 未提交”写成“最终客户端零字节”。

## 7. 文档决策记录

| 日期 | 责任与状态 | 修订 |
|---|---|---|
| 2026-10-20 | 主会话，待用户 review | 以 00 v0.9 补齐 opener 前统一预算、Done／Error 原因、非流式直接 Chat、真实提交状态、阻塞 Write／Flush 退出及入口异常中止分工；纠正旧稿 Overall 必已提交及两段计时的假设。未实现／未验收 |
| 2026-10-20 | 用户确认，主会话登记冻结 | 用户在主会话确认 B3 文档并要求编写 13.1 开发 prompt；本施工要求与 00 v0.9／01 v0.4 同步冻结，保留候选历史，不新增业务要求；尚无实现或独立代码审查结论 |
| 2026-10-20 | 主会话，前置交付与开工准备 | 13.2 已经返工、独立复审、合并及推送，证据见 13.2 §11.12～§11.14。下一份按已冻结的 15.1 施工，15.2 接续完整预算与阻塞写保证；本次仅登记前置进度并准备 prompt，不增加要求，不宣称本模块已实现或 M0 完成 |
| 2026-10-21 | 主会话，M1 衔接裁定，待用户随 M2 确认 | 接受 M1 作为已报告范围的阶段交付，不当作独立代码审查。为兑现原包装链正向验收，按 00 §6.9 第7项把标准 Unwrap 最小透明能力及回归前移至 15.1 M2；不降低验收，15.2／13.3 其余职责不变，尚未实现该补充或完成完整 L2 |
| 2026-10-21 | 主会话，15.1 收尾与 15.2 开工准备 | 核对15.1 §11.7／§11.12～§11.13：最小前移已获用户确认，独立审查可合并，合并39f420b、收尾03ba601与本地origin/main同值；关闭台账#27。下一份15.2沿既有冻结契约施工，不改公开签名、不提前实现13.3，模块整体／B3／M0仍未验收 |
| 2026-10-21 | 主会话归因裁定，待用户转交确认 | 按00 §6.9第8项／15.2 §3.4限定父取消优先，允许已确定Overall原因锁存及受限写超时补判；错误链与Kind分开消费。本次只补契约，不宣称实现或独立验收通过 |
| 2026-10-21 | 主会话，归因确认与审查准备 | 根据15.2 §11.7及用户转交记录确认9f9450c已获批准，核对实际交付1dc1f88，更新15.2已实现待独立审查；保留原裁定历史，不新增规则，不把实现自测当作独立结论 |
| 2026-10-21 | 主会话，15.2验收收尾 | 核对15.2 §11.17～§11.18，原P2经返工及独立复审关闭，合并b9740e9、收尾9d11eac与本地origin/main同值，实现与c5ebd7c一致。两个L2均已交付，下一份按B3顺序为13.4；13.3正式装配和B3／M0仍未验收，#25／#26不关闭 |
| 2026-10-22 | 用户授权交付登记，正式网关集成完成 | 13.3独立限定／race／全仓真实SQL通过，真进程正常／中断、头前后预算、取消／真实慢读、Recover／摘要与SIGTERM均有证据；用户授权后合并b21c85995b7a35fa145009bf6e91b786ffa336e4并实时核对远端main同值，B3／M0验收登记。15.1／15.2冻结语义及历史记录不变；#25未来16.4／18.1消费仍open，20.1全链部署未提前验收 |

## B5多attempt消费（2026-10-30候选，待review）

16.4按00 §6.11矩阵消费ForwardResult：提交状态、Kind、UpstreamError.Retryable及原客户端／逻辑剩余预算共同裁定，不能只看首字节或错误链Canceled。每次opener必须使用proxy传入ctx，一次逻辑总预算先覆盖Route，多个attempt不刷新；FirstByteMs仍是单次方法起点到可转发载荷，不再次加连接时间。所有Stream.Close／定时器／下游控制完成后才换候选，并在同一真实WrapWriter／Gin／TCP上验证多attempt头／deadline清理。provider／proxy／middleware签名及已冻结归因不改；接缝缺口交主会话及原owner，不由16.4暗改。B6后续计量边界仍未由本候选提前实现。
