package store

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

// 落账队列参数（11.3 §3/§4 决策②）：条数或时间先到者触发批量写入——
// 低流量时 settleFlushInterval 保证及时性，高流量时 settleBatchSize 摊薄开销。
const (
	defaultSettleQueueSize = 1024                   // MeterConfig.SettleQueueSize 缺省（11.3 §3 / 10.1）
	settleBatchSize        = 100                    // 单批最大条数
	settleFlushInterval    = 500 * time.Millisecond // 攒批上限时长
	settleBatchMaxRetries  = 3                      // 批量写入失败后的最大重试次数（共 4 次尝试）
	settleInsertTimeout    = 5 * time.Second        // 单次批量写入超时（防 DB 卡死拖住 drain）
	settleDrainPoll        = 2 * time.Millisecond   // Close 等待队列静默的轮询间隔
)

// UsageRepo 是 usage_records 的写入层（11.3 §3 冻结签名）。
//
// 核心契约：Record 永不阻塞、永不返回业务错误——入队即返回；队列满或落库
// 重试耗尽时丢弃该条并触发 OnDrop（11.3 §4 决策①：丢弃可观测而非假装不丢）。
// usage_records 只追加、不提供 Update/Delete（11-L1 §3 决策②，账目不可变）。
//
// 消费方：17.1/17.2/17.3（meter）；Close 由 10.2 优雅关闭序列第一步调用。
//
// TODO(17.1): 构造 UsageRecord 时按 models.go 的字段类型（Estimated 为 int8 0/1），
// 11.3 §3 的 Go 片段类型与 11.1 §9.7 冻结映射的一致性见 11.3 §9 实现记录① / 02 台账 #15。
type UsageRepo interface {
	// Record 入队一条 usage_records。永不阻塞、无返回值。队列满或已关闭时
	// 丢弃该记录并调用 OnDrop。CreatedAt 为零值时按调用时刻（UTC）补齐。
	Record(rec UsageRecord)
	// SetOnDrop 设置丢弃回调（默认 Error 日志；B7 由 18 接管为打点）。
	SetOnDrop(fn func(rec UsageRecord))
	// Close drain：等待已入队记录全部落库后退出（10.2 关闭序列第一步——
	// 必须先于关闭 DB 连接调用，否则队列中未落库记录丢失）。幂等。
	Close(ctx context.Context) error
}

// usageRepo 是 UsageRepo 的 MySQL 实现：单 worker goroutine 消费缓冲 channel，
// 批量事务写入 usage_records。
type usageRepo struct {
	db    *gorm.DB
	queue chan UsageRecord

	mu   sync.RWMutex // 保护 done：Close 屏障与 Record 的入队临界区
	done bool         // Close 屏障后为真；此后 Record 走丢弃路径（不再入队）

	dropMu sync.Mutex     // 保护 onDrop 回调的读写
	onDrop func(UsageRecord)

	pending   atomic.Int64 // 已入队但未落库（含写入中）的条数，Close 据此判静默
	dropped   atomic.Int64 // 累计丢弃条数（诚实计数，用例②/18 打点用）
	closeOnce sync.Once
	closeErr  error

	workerDone chan struct{} // worker 退出时关闭

	// 仅测试用钩子（生产恒 nil；须在 start 之前注入）：
	testHookFlush func(batch []UsageRecord) // 批量写入前调用，供断言批次大小
	testHookDelay func()                    // 在写入前人为延迟，模拟慢 DB
}

// newUsageRepo 构造落账仓库但不启动 worker（测试可在 start 前注入钩子）。
func newUsageRepo(db *gorm.DB, queueSize int) *usageRepo {
	if queueSize <= 0 {
		queueSize = defaultSettleQueueSize
	}
	return &usageRepo{
		db:         db,
		queue:      make(chan UsageRecord, queueSize),
		onDrop:     defaultOnDrop,
		workerDone: make(chan struct{}),
	}
}

// NewUsageRepo 装配落账队列并启动后台 worker（11.3 §3）。
// queueSize 来自 MeterConfig.SettleQueueSize（默认 1024）。
func NewUsageRepo(db *gorm.DB, queueSize int) UsageRepo {
	r := newUsageRepo(db, queueSize)
	r.start()
	return r
}

// start 启动后台消费 worker（非请求路径，01 §3 的请求路径裸 go 禁令不适用）。
func (r *usageRepo) start() {
	go r.run()
}

// defaultOnDrop 是 OnDrop 的缺省实现：Error 日志（B7 由 18 接管为
// gaigate_usage_drop_total 打点，11.3 §3/§7）。
func defaultOnDrop(rec UsageRecord) {
	slog.Error("usage_record_dropped",
		"request_id", rec.RequestID,
		"user_id", rec.UserID,
		"channel_id", rec.ChannelID,
		"model", rec.Model,
	)
}

// run 是批量消费循环：阻塞取首条 → 攒批（最多 settleBatchSize 条，或
// settleFlushInterval 到点）→ 批量写入。队列关闭（Close 屏障）后把缓冲
// 中剩余记录全部排空再退出。
func (r *usageRepo) run() {
	defer close(r.workerDone)
	for {
		rec, ok := <-r.queue
		if !ok {
			return // 队列已关闭且已排空
		}
		batch := make([]UsageRecord, 0, settleBatchSize)
		batch = append(batch, rec)
		timer := time.NewTimer(settleFlushInterval)
	fill:
		for len(batch) < settleBatchSize {
			select {
			case rec2, ok2 := <-r.queue:
				if !ok2 {
					break fill
				}
				batch = append(batch, rec2)
			case <-timer.C:
				break fill
			}
		}
		timer.Stop()
		r.flush(batch)
	}
}

// flush 批量写入一条已攒批的记录集合：失败重试 settleBatchMaxRetries 次，
// 仍失败则逐条丢弃（OnDrop）。无论成败都从 pending 中扣减。
func (r *usageRepo) flush(batch []UsageRecord) {
	if len(batch) == 0 {
		return
	}
	if r.testHookFlush != nil {
		r.testHookFlush(batch)
	}
	if r.testHookDelay != nil {
		r.testHookDelay()
	}
	var lastErr error
	for attempt := 0; attempt <= settleBatchMaxRetries; attempt++ {
		if lastErr = r.insertBatch(batch); lastErr == nil {
			break
		}
	}
	if lastErr != nil {
		for i := range batch {
			r.reportDrop(batch[i])
		}
	}
	r.pending.Add(-int64(len(batch)))
}

// insertBatch 在一个事务内批量插入（11.3 §3：批量 INSERT（事务））。
// 单次写入带超时，避免 DB 卡死无限拖住 drain。
func (r *usageRepo) insertBatch(batch []UsageRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), settleInsertTimeout)
	defer cancel()
	if err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(&batch).Error
	}); err != nil {
		return fmt.Errorf("store: 批量写入 usage_records(%d 条): %w", len(batch), err)
	}
	return nil
}

// reportDrop 记一次丢弃：累计计数 + 调用 OnDrop 回调。
func (r *usageRepo) reportDrop(rec UsageRecord) {
	r.dropped.Add(1)
	r.dropMu.Lock()
	fn := r.onDrop
	r.dropMu.Unlock()
	if fn != nil {
		fn(rec)
	}
}

// Record 见 UsageRepo。入队即返回：永不阻塞（非阻塞 send），永不返回错误。
func (r *usageRepo) Record(rec UsageRecord) {
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	r.mu.RLock()
	if r.done { // Close 屏障之后：丢弃
		r.mu.RUnlock()
		r.reportDrop(rec)
		return
	}
	r.pending.Add(1)
	select {
	case r.queue <- rec:
		r.mu.RUnlock()
	default: // 队列满：丢弃并打点（11.3 §4 决策①）
		r.pending.Add(-1)
		r.mu.RUnlock()
		r.reportDrop(rec)
	}
}

// SetOnDrop 见 UsageRepo。回调由 worker 与本方法在 dropMu 下交互，调用方
// 不应在回调内长时间阻塞（其在批量失败路径上被逐条调用）。
func (r *usageRepo) SetOnDrop(fn func(rec UsageRecord)) {
	r.dropMu.Lock()
	r.onDrop = fn
	r.dropMu.Unlock()
}

// Close 见 UsageRepo。drain 语义（11.3 §3 / 10.2 §3.2）：
//  ① 等待队列静默（pending 归零）——Close 期间并发 Record 的记录也在此窗口落库；
//  ② 屏障：置 done 阻断新入队（此后 Record 走丢弃路径），关闭队列；
//  ③ 等 worker 排空剩余并退出。
//
// ctx 耗尽即提前返回错误，但已入队记录仍由 worker 后台排空落库。幂等。
func (r *usageRepo) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		r.closeErr = r.drain(ctx)
	})
	return r.closeErr
}

func (r *usageRepo) drain(ctx context.Context) error {
	// ① 等待队列静默：pending 为已入队未落库条数，归零即全部落库。
waitQuiet:
	for r.pending.Load() != 0 {
		select {
		case <-ctx.Done():
			break waitQuiet // 超时：不再等待，直接进入屏障（worker 关闭队列后仍会排空）
		case <-time.After(settleDrainPoll):
		}
	}
	// ② 屏障：持写锁置 done，阻断并发的入队临界区，再关闭队列。
	r.mu.Lock()
	r.done = true
	r.mu.Unlock()
	close(r.queue)
	// ③ 等 worker 排空剩余记录并退出。
	select {
	case <-r.workerDone:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
