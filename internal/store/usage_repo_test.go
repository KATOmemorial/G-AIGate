package store

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// testCtx 返回带超时的测试 context（用例末尾自动取消）。
func testCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// countUsage 返回 usage_records 当前行数。
func countUsage(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&UsageRecord{}).Count(&n).Error)
	return n
}

// usageRec 构造一条 usage_records（字段与表一一对应，见 models.go）。
func usageRec(i int) UsageRecord {
	return UsageRecord{
		RequestID:    "req_test_" + strconv.Itoa(i),
		UserID:       1,
		APIKeyID:     2,
		ChannelID:    3,
		Model:        "gpt-4o-demo",
		InputTokens:  uint32(10 + i),
		OutputTokens: uint32(5 + i),
		Cost:         int64(100 + i),
		LatencyMS:    uint32(20 + i),
		Status:       UsageStatusSuccess,
		Estimated:    0,
		CreatedAt:    time.Now().UTC(),
	}
}

// §5 用例①：Record 1000 条 → Close 后 SELECT COUNT(*) == 1000（容量 1024 内无丢弃）。
func TestUsageRepo_RecordDrainsAll(t *testing.T) {
	db := migratedTestDB(t)
	r := newUsageRepo(db, defaultSettleQueueSize)
	r.start()

	for i := 0; i < 1000; i++ {
		r.Record(usageRec(i))
	}
	require.NoError(t, r.Close(testCtx(t, 5*time.Second)))

	assert.Equal(t, int64(1000), countUsage(t, db), "Close 后应全部落库")
	assert.Zero(t, r.dropped.Load(), "容量内不应有丢弃")

	// 字段落库正确（行为级断言，非仅行数）
	var got UsageRecord
	require.NoError(t, db.Where("request_id = ?", "req_test_0").First(&got).Error)
	assert.Equal(t, "gpt-4o-demo", got.Model)
	assert.Equal(t, int64(100), got.Cost)
	assert.Equal(t, UsageStatusSuccess, got.Status)
}

// §5 用例②：队列灌满（容量 1024，灌 5000 条瞬间）→ 丢弃数 == OnDrop 回调数，不死锁。
func TestUsageRepo_QueueFullDropsAndOnDrop(t *testing.T) {
	db := migratedTestDB(t)
	r := newUsageRepo(db, defaultSettleQueueSize)

	var cbCount atomic.Int64
	r.SetOnDrop(func(UsageRecord) { cbCount.Add(1) })
	r.start()

	const total = 5000
	for i := 0; i < total; i++ {
		r.Record(usageRec(i)) // 非阻塞，灌满是必然
	}
	require.NoError(t, r.Close(testCtx(t, 10*time.Second)), "灌满后 Close 不得死锁")

	persisted := countUsage(t, db)
	dropped := r.dropped.Load()
	assert.Equal(t, int64(total), persisted+dropped, "落库数 + 丢弃数 == 灌入总数")
	assert.Greater(t, dropped, int64(0), "容量 1024 < 5000，必有丢弃")
	assert.Equal(t, dropped, cbCount.Load(), "丢弃数 == OnDrop 回调次数")
}

// §5 用例③：worker 批量——注入慢 DB（写入前延迟）观察批量行为（hook 断言批次大小）。
func TestUsageRepo_BatchFlush(t *testing.T) {
	db := migratedTestDB(t)
	r := newUsageRepo(db, defaultSettleQueueSize)

	var mu sync.Mutex
	var sizes []int
	r.testHookFlush = func(batch []UsageRecord) {
		mu.Lock()
		sizes = append(sizes, len(batch))
		mu.Unlock()
	}
	r.testHookDelay = func() { time.Sleep(30 * time.Millisecond) } // 模拟慢 DB
	r.start()

	const total = 250
	for i := 0; i < total; i++ {
		r.Record(usageRec(i))
	}
	require.NoError(t, r.Close(testCtx(t, 10*time.Second)))

	mu.Lock()
	defer mu.Unlock()
	sum, maxSize := 0, 0
	for _, s := range sizes {
		sum += s
		if s > maxSize {
			maxSize = s
		}
	}
	assert.Equal(t, total, sum, "各批次条数之和 == 记录总数")
	assert.Equal(t, settleBatchSize, maxSize, "应出现满批（100 条）——证明批量而非逐条写")
	assert.Greater(t, len(sizes), 1, "应至少分多批写入")
	assert.LessOrEqual(t, len(sizes), 6, "批次数应远小于记录数（250）")
	assert.Equal(t, int64(total), countUsage(t, db))
}

// §5 用例④：Close 期间继续 Record → 全部落库（drain 边界）。
func TestUsageRepo_CloseDuringDrainKeepsAll(t *testing.T) {
	db := migratedTestDB(t)
	r := newUsageRepo(db, defaultSettleQueueSize)
	r.testHookDelay = func() { time.Sleep(100 * time.Millisecond) } // 拖长 drain 窗口
	r.start()

	r.Record(usageRec(0)) // 使 pending>0，撑开 drain 窗口
	closed := make(chan error, 1)
	go func() { closed <- r.Close(testCtx(t, 10*time.Second)) }()

	// Close 进行中继续 Record（drain 边界：屏障前的记录都应落库）
	const extra = 200
	for i := 1; i <= extra; i++ {
		r.Record(usageRec(i))
	}
	require.NoError(t, <-closed)

	assert.Equal(t, int64(extra+1), countUsage(t, db), "Close 期间 Record 的记录应全部落库")
	assert.Zero(t, r.dropped.Load(), "drain 窗口内的记录不应被丢弃")
}
