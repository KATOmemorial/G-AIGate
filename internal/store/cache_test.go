package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// queryCountingLogger 统计 GORM 执行的 SQL 条数：GORM 每执行一条语句调用一次
// Trace（该方法先于日志级别过滤），故比回调 hook 更可靠地覆盖 Find/First/Scan/Raw。
type queryCountingLogger struct{ n int64 }

func (l *queryCountingLogger) LogMode(logger.LogLevel) logger.Interface { return l }
func (l *queryCountingLogger) Info(context.Context, string, ...interface{}) {
}
func (l *queryCountingLogger) Warn(context.Context, string, ...interface{}) {
}
func (l *queryCountingLogger) Error(context.Context, string, ...interface{}) {
}
func (l *queryCountingLogger) Trace(context.Context, time.Time, func() (string, int64), error) {
	atomic.AddInt64(&l.n, 1)
}

// countQueries 把 db 的 logger 换成计数 logger，返回计数指针，用于断言
// "缓存命中不产生 SQL"（11.2 §5 用例②）。须在断言区间内无其他 DB 写操作。
func countQueries(t *testing.T, db *gorm.DB) *int64 {
	t.Helper()
	cl := &queryCountingLogger{}
	db.Logger = cl
	return &cl.n
}

// §5 用例②：缓存命中——第二次及以后调用不产生 SQL
func TestCache_HitNoSQL(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db, NewConfigCache(time.Minute))
	n := countQueries(t, db)
	ctx := context.Background()

	first, err := repo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	require.Len(t, first, 2)
	assert.Equal(t, int64(1), atomic.LoadInt64(n), "首次读应回源一次")

	for i := 0; i < 5; i++ {
		got, err := repo.ListActiveByModel(ctx, "gpt-4o-demo")
		require.NoError(t, err)
		assert.Len(t, got, 2)
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(n), "TTL 内命中缓存，不应再产生 SQL")
}

// §5 用例③：InvalidateChannels 后立即读到变更值（无 60s 等待）
func TestCache_InvalidateChannels(t *testing.T) {
	db := seededTestDB(t)
	c := NewConfigCache(time.Minute)
	repo := NewChannelRepo(db, c)
	ctx := context.Background()

	first, err := repo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, int32(10), first[0].Priority)

	// 直接改库（绕过 repo，不失效）→ 读仍是缓存旧值
	require.NoError(t, db.Model(&Channel{}).Where("name = ?", "demo-primary").
		Update("priority", 99).Error)
	stale, err := repo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, int32(10), stale[0].Priority, "未失效前应返回缓存旧值")

	// 失效后下一次读穿透回源，立即拿到新值
	c.InvalidateChannels("gpt-4o-demo")
	fresh, err := repo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, int32(99), fresh[0].Priority, "失效后应立即读到变更值")
}

// §5 用例④：并发 50 goroutine 同 key 冷缓存读，DB 查询次数 == 1（singleflight）
func TestCache_SingleflightDBQueries(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db, NewConfigCache(time.Minute))
	n := countQueries(t, db)

	const goroutines = 50
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			chs, err := repo.ListActiveByModel(context.Background(), "gpt-4o-demo")
			if err != nil {
				errs <- err
				return
			}
			if len(chs) != 2 {
				errs <- fmt.Errorf("结果数应为 2，实际 %d", len(chs))
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(n),
		"50 并发同 key 冷缓存：DB 查询应恰为 1（并发回源被 singleflight 合并）")
}

// 用例④ 的机制证据：并发回源被合并为一次（阻塞式 load，未合并实现会有 50 次调用）
func TestCache_SingleflightMergesLoads(t *testing.T) {
	c := NewConfigCache(time.Minute)
	var loads int64
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	load := func(context.Context) ([]Channel, error) {
		atomic.AddInt64(&loads, 1)
		once.Do(func() { close(started) })
		<-release // 阻塞首个回源，制造并发窗口
		return []Channel{{ID: 1, Name: "x"}}, nil
	}

	const goroutines = 50
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = getOrLoad(c, context.Background(), "k", load, cloneChannels)
		}()
	}
	close(start)
	<-started
	// 首个回源已进入；给未合并实现足够时间让其余 goroutine 也进入回源
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(1), atomic.LoadInt64(&loads), "并发回源必须被合并为一次")

	close(release)
	wg.Wait()
}

// 值拷贝：调用方修改返回值（标量/slice/指针字段）不得污染缓存
func TestCache_ValueCopyNoPollution(t *testing.T) {
	db := seededTestDB(t)
	c := NewConfigCache(time.Minute)
	chRepo := NewChannelRepo(db, c)
	mRepo := NewModelRepo(db, c)
	pRepo := NewPriceRepo(db, c)
	ctx := context.Background()

	// 给渠道一个非空 LastCheckedAt 以验证指针字段深拷贝
	require.NoError(t, db.Model(&Channel{}).Where("name = ?", "demo-primary").
		Update("last_checked_at", time.Now().UTC()).Error)

	chs, err := chRepo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	require.NotNil(t, chs[0].LastCheckedAt)
	origChecked := *chs[0].LastCheckedAt
	// 污染：改标量、改指针指向、覆盖 slice 元素
	chs[0].Priority = 4242
	chs[0].Name = "polluted"
	*chs[0].LastCheckedAt = chs[0].LastCheckedAt.Add(time.Hour)
	chs[0] = Channel{Name: "overwritten"}

	again, err := chRepo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, "demo-primary", again[0].Name, "标量字段不应被污染")
	assert.Equal(t, int32(10), again[0].Priority)
	require.NotNil(t, again[0].LastCheckedAt)
	assert.True(t, again[0].LastCheckedAt.Equal(origChecked), "指针字段应深拷贝，不被污染")

	// 别名：*uint64 与字符串字段
	a1, err := mRepo.Resolve(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	require.NotNil(t, a1.DefaultChannelID)
	*a1.DefaultChannelID = 9999
	a1.FallbackIDs = "[9999]"
	a2, err := mRepo.Resolve(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	require.NotNil(t, a2.DefaultChannelID)
	assert.NotEqual(t, uint64(9999), *a2.DefaultChannelID, "别名指针字段不应被污染")
	assert.Equal(t, []uint64{2}, ParseFallbackIDs(a2.FallbackIDs))

	// 价格
	p1, err := pRepo.Effective(ctx, "gpt-4o-demo", time.Now().UTC())
	require.NoError(t, err)
	p1.InputPricePerM = 1
	p2, err := pRepo.Effective(ctx, "gpt-4o-demo", time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, int64(15_000_000), p2.InputPricePerM, "价格对象不应被污染")
}

// TTL 边界：过期后下一次读重新回源（注入时钟，无需真实等待 60s）
func TestCache_TTLExpiry(t *testing.T) {
	db := seededTestDB(t)
	c := NewConfigCache(time.Minute)
	clock := time.Now()
	c.now = func() time.Time { return clock }
	repo := NewChannelRepo(db, c)
	n := countQueries(t, db)
	ctx := context.Background()

	_, err := repo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	_, err = repo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(n), "TTL 内应命中缓存")

	clock = clock.Add(61 * time.Second) // 越过 60s TTL
	_, err = repo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, int64(2), atomic.LoadInt64(n), "TTL 过期后应重新回源")
}

// InvalidateAlias / InvalidateAll 语义
func TestCache_InvalidateAliasAndAll(t *testing.T) {
	db := seededTestDB(t)
	c := NewConfigCache(time.Minute)
	chRepo := NewChannelRepo(db, c)
	mRepo := NewModelRepo(db, c)
	ctx := context.Background()

	_, err := chRepo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	_, err = mRepo.Resolve(ctx, "gpt-4o-demo")
	require.NoError(t, err)

	// 改库后未失效 → 旧值
	require.NoError(t, db.Model(&ModelAlias{}).Where("name = ?", "gpt-4o-demo").
		Update("strategy", "cost_first").Error)
	stale, err := mRepo.Resolve(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, "weighted", stale.Strategy)

	c.InvalidateAlias("gpt-4o-demo")
	fresh, err := mRepo.Resolve(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, "cost_first", fresh.Strategy, "InvalidateAlias 应立即生效")

	// InvalidateAll 同时清渠道与别名
	require.NoError(t, db.Model(&ModelAlias{}).Where("name = ?", "gpt-4o-demo").
		Update("strategy", "latency_first").Error)
	require.NoError(t, db.Model(&Channel{}).Where("name = ?", "demo-primary").
		Update("priority", 77).Error)
	c.InvalidateAll()

	f2, err := mRepo.Resolve(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, "latency_first", f2.Strategy, "InvalidateAll 应清别名缓存")
	ch2, err := chRepo.ListActiveByModel(ctx, "gpt-4o-demo")
	require.NoError(t, err)
	assert.Equal(t, int32(77), ch2[0].Priority, "InvalidateAll 应清渠道缓存")
}
