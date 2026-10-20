# 15-proxy（L1）

> 状态：已冻结（2026-10-20 用户确认 B3 文档；尚未实现） | 层：第 1 层数据面 | 批次：B3
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

proxy 关闭上游并返回错误；HTTP handler 决定未提交时写错误、已提交时 ErrAbortHandler 异常终止。proxy 自身不 recover／panic，不向流内塞错误帧。13.3 对接外层 Recover。

## 3. 关键设计决策

1. **应用层 data 保真**：适配器给出 Raw data，proxy 按 SSE 规则重建 data 行和空行；不把 Chunk 解析再序列化。HTTP 分块、换行风格、注释等不保证逐字节相同。Done 显式转发一次；内容／usage／Done 分别计数，Chunks 不包含后两者。
2. **同一尝试预算**：进入 Forward／ForwardNonStream，在 opener 前计时。TTFB 覆盖建连、响应头及首个合法可转发 data（非流式为完整 JSON）；首个有效载荷就绪后停 TTFB。Overall 含慢客户端 Write／Flush 和正常清理。FirstByteMs 是应用层可转发时刻，未出现为 -1，不是客户端收到时间。
3. **有界链条**：适配器 worker → 固定容量 channel → 同步转发写出；慢写让 channel 写满，停止继续解析上游。不能用每帧启动 goroutine 或无界队列绕开反压。
4. **阻塞写必须退出**：在 commit 前确认底层 writer 的错误 Flush／写 deadline 能力；Overall 写期限和取消后的主动打断覆盖正在阻塞的 Write／Flush。所有 watcher 与 worker 有唯一 owner 并等待退出。生产链不支持能力时，在未提交状态报错，不能静默声称有保证。
5. **非流式直接 Chat**：完整 JSON 有界读取并一次转发，不用 SSE 缓冲伪造非流式。缺 usage 可由未来 meter 估算；错误 JSON、空响应或超限不提交成功头。

## 4. L2 清单与边界

- [15.1 SSE 与非流式转发](15-proxy/15.1-sse-forward.md)：opener 接口、内容／usage／Done、提交标记、错误分类、Write／短写／Flush 停止、关闭与同步回调。继承父 context 取消；双预算和阻塞写完整保证在 15.2。
- [15.2 背压／取消／双超时](15-proxy/15.2-backpressure-cancel-timeout.md)：不更改 15.1 对外签名，补齐 opener 前预算、底层 deadline、worker／watcher 收敛及真实慢读者验证。

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
