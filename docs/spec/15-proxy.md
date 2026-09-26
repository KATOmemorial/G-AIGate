# 15-proxy（L1）

> 状态：B1 评审中 | 层：第 1 层数据面 | 批次：B3
> 上游：[00-总纲](00-总纲.md) §3.2 / §5.2 | 工程基线：[01-工程规范](01-工程规范.md)
> 流式是地基而非收尾（00 §11 v0.1 决策④）——本模块与 access 同批交付。

---

## 1. 模块边界

**做什么**：SSE 流式转发机制（透传、Flush 时机、响应头管理）；背压（客户端慢时不无限缓冲）；取消传播（客户端断开 → 取消上游请求）；双超时（首字节与整体分离）；非流式缓冲路径。

**不做什么**：候选迭代与切换决策（16.4——本模块只报告"这次转发结果如何"，不决定"要不要换渠道"）；计量累计（17——通过分片回调把数据交出去）。

## 2. 接口（模块内核心类型，15.1 冻结签名）

```go
// ForwardResult 的结果分类是 16.4 切换决策的输入（契约级语义）
// Success / TTFBTimeout（未写响应头，可切换）/ StreamInterrupted（已写出，不可切换）
// OverallTimeout（已写出，不可切换）/ ClientCanceled

type Forwarder interface {
    Forward(ctx context.Context, w http.ResponseWriter, s provider.Stream,
        opts ForwardOptions, onChunk func(ev provider.StreamEvent)) (ForwardResult, error)
}
```

## 3. 关键设计决策

1. **逐 chunk write + flush，不聚合**：低延迟优先。背压链条是本模块的面试核心题：客户端消费慢 → `w.Write` 阻塞 → 有界 channel（64 chunk）写满 → 停止从上游读取 → 上游 socket 发送缓冲堆积 → TCP 窗口收缩 → 上游暂停发送。全链无无限缓冲。
2. **取消传播**：`r.Context().Done()`（客户端断开）→ `stream.Close()`（13.2 的 Provider 层取消上游 HTTP 请求）→ goroutine 收敛，无泄漏。验收用 mock 侧连接日志验证。
3. **双超时语义**：TTFB 超时（默认 10s）发生在写响应头**之前**，是可切换信号；整体超时（默认 300s）只保证资源回收，此时响应头已写出，不可切换——这条边界是 00 §3.2.1 的实现者。
4. **非流式路径复用同一管道**：`stream=false` 时缓冲全部 chunk 后一次性写出，错误映射与流式一致。

## 4. L2 清单（与 00 §4.1 预排一致，无调整）

- **15.1 SSE 流式透传与非流式路径**：头管理（`Content-Type: text/event-stream`、禁用压缩与缓冲）、Flush、`ForwardResult` 分类、非流式缓冲。
- **15.2 背压 / 取消传播 / 双超时**：有界缓冲、取消链、两种超时计时器。

## 5. 主要风险

- 中间件包装 `ResponseWriter` 导致 `http.Flusher` 断言失败——提供包装器透传 Flusher，验收覆盖。
- 极慢客户端长期占用连接：整体超时 300s 兜底，连接数指标（18）监控。

## 6. 验收要点

B3 检查点贡献：`curl -N` 流式逐行可见；客户端 Ctrl+C 后 mock 日志显示上游连接被关闭（取消传播生效）；mock `slow` 场景 TTFB 超时触发且未向客户端写出任何字节（可切换路径成立）。
