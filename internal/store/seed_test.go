package store

import (
	"database/sql"
	"strconv"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"gaigate/internal/secret"
)

func testMasterKey() []byte {
	// 32 字节测试主密钥（与 GAIGATE_SECRET_MASTER_KEY 解码后等价）
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i + 1)
	}
	return k
}

// migratedTestDB 迁移完毕的测试库 GORM 连接。
func migratedTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	opts := testOptions(t)
	recreateTestDatabase(t, opts)
	if err := RunMigrations(opts, "../../migrations"); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

// rowCounts 抓取 seed 涉及的全部表的行数，用于幂等断言。
func rowCounts(t *testing.T, db *gorm.DB) map[string]int64 {
	t.Helper()
	type entry struct {
		Table string
		Cnt   int64
	}
	var rows []entry
	// GROUPS 在 MySQL 8.0+ 为保留字，原生 SQL 须反引号（GORM 生成查询会自动加）
	if err := db.Raw("SELECT 'groups' AS `table`, COUNT(*) AS cnt FROM `groups`" +
		" UNION ALL SELECT 'users', COUNT(*) FROM users" +
		" UNION ALL SELECT 'channels', COUNT(*) FROM channels" +
		" UNION ALL SELECT 'channel_models', COUNT(*) FROM channel_models" +
		" UNION ALL SELECT 'channel_keys', COUNT(*) FROM channel_keys" +
		" UNION ALL SELECT 'model_aliases', COUNT(*) FROM model_aliases" +
		" UNION ALL SELECT 'model_prices', COUNT(*) FROM model_prices",
	).Scan(&rows).Error; err != nil {
		t.Fatalf("统计行数: %v", err)
	}
	out := map[string]int64{}
	for _, r := range rows {
		out[r.Table] = r.Cnt
	}
	return out
}

// keyCiphertexts 抓取两渠道 Key 密文，用于断言重启零重写。
func keyCiphertexts(t *testing.T, db *gorm.DB) [][]byte {
	t.Helper()
	var keys []ChannelKey
	if err := db.Order("id").Find(&keys).Error; err != nil {
		t.Fatalf("查询 channel_keys: %v", err)
	}
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		out = append(out, k.ProviderKeyEnc)
	}
	return out
}

func TestSeed_CreatesExpectedRows(t *testing.T) {
	db := migratedTestDB(t)
	opts := SeedOptions{AdminPassword: "admin-test", MasterKey: testMasterKey()}
	if err := Seed(db, opts); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// 渠道规格（11.1 §3 seed 表格）
	var channels []Channel
	if err := db.Order("id").Find(&channels).Error; err != nil {
		t.Fatalf("查询 channels: %v", err)
	}
	if len(channels) != 2 {
		t.Fatalf("渠道数应为 2，实际 %d", len(channels))
	}
	primary, fallback := channels[0], channels[1]
	if primary.Name != "demo-primary" || primary.Priority != 10 || primary.Weight != 70 {
		t.Errorf("demo-primary 规格不符: %+v", primary)
	}
	if fallback.Name != "demo-fallback" || fallback.Priority != 5 || fallback.Weight != 30 {
		t.Errorf("demo-fallback 规格不符: %+v", fallback)
	}
	if primary.Provider != "openai" || primary.BaseURL != "http://127.0.0.1:18080" {
		t.Errorf("渠道 provider/base_url 不符: %+v", primary)
	}

	// admin 用户：default 组、admin 角色、密码 bcrypt 可验
	var admin User
	if err := db.Where("username = ?", "admin").First(&admin).Error; err != nil {
		t.Fatalf("查询 admin: %v", err)
	}
	var grp Group
	if err := db.Where("name = ?", "default").First(&grp).Error; err != nil {
		t.Fatalf("查询 default 组: %v", err)
	}
	if admin.Role != RoleAdmin || admin.GroupID != grp.ID || admin.Status != UserStatusEnabled {
		t.Errorf("admin 规格: %+v (group=%d)", admin, grp.ID)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(admin.PasswordHash), []byte("admin-test")); err != nil {
		t.Errorf("admin 密码 bcrypt 校验失败: %v", err)
	}

	// 渠道 Key：密文可用 master key 解密回明文
	var keys []ChannelKey
	if err := db.Order("id").Find(&keys).Error; err != nil {
		t.Fatalf("查询 channel_keys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("渠道 Key 数应为 2，实际 %d", len(keys))
	}
	got, err := secret.Decrypt(keys[0].ProviderKeyEnc, testMasterKey())
	if err != nil || string(got) != "sk-mock-a" {
		t.Errorf("渠道 1 Key 解密: got %q err %v", got, err)
	}
	got, err = secret.Decrypt(keys[1].ProviderKeyEnc, testMasterKey())
	if err != nil || string(got) != "sk-mock-b" {
		t.Errorf("渠道 2 Key 解密: got %q err %v", got, err)
	}

	// 模型映射：两渠道均服务 gpt-4o-demo（上游同名）
	var cms []ChannelModel
	if err := db.Find(&cms).Error; err != nil {
		t.Fatalf("查询 channel_models: %v", err)
	}
	if len(cms) != 2 {
		t.Fatalf("channel_models 应 2 行，实际 %d", len(cms))
	}
	for _, cm := range cms {
		if cm.Model != "gpt-4o-demo" || cm.UpstreamModel != "gpt-4o-demo" {
			t.Errorf("channel_model 规格不符: %+v", cm)
		}
	}

	// 别名：default=渠道1，fallback=[渠道2]，weighted
	var alias ModelAlias
	if err := db.Where("name = ?", "gpt-4o-demo").First(&alias).Error; err != nil {
		t.Fatalf("查询 model_aliases: %v", err)
	}
	if alias.DefaultChannelID == nil || *alias.DefaultChannelID != primary.ID {
		t.Errorf("别名 default_channel_id 应为渠道 %d，实际 %v", primary.ID, alias.DefaultChannelID)
	}
	if want := "[" + strconv.FormatUint(fallback.ID, 10) + "]"; alias.FallbackIDs != want {
		t.Errorf("别名 fallback_ids 应为 %s，实际 %s", want, alias.FallbackIDs)
	}
	if alias.Strategy != "weighted" {
		t.Errorf("别名 strategy 应为 weighted，实际 %s", alias.Strategy)
	}

	// 价格：15M/60M 微元/M
	var price ModelPrice
	if err := db.Where("model = ?", "gpt-4o-demo").First(&price).Error; err != nil {
		t.Fatalf("查询 model_prices: %v", err)
	}
	if price.InputPricePerM != 15_000_000 || price.OutputPricePerM != 60_000_000 ||
		price.Currency != "CNY" || !price.Enabled {
		t.Errorf("价格规格不符: %+v", price)
	}
	if !price.EffectiveFrom.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("effective_from 应为纪元零点，实际 %v", price.EffectiveFrom)
	}
}

func TestSeed_IdempotentOnRestart(t *testing.T) {
	db := migratedTestDB(t)
	opts := SeedOptions{AdminPassword: "admin-test", MasterKey: testMasterKey()}
	if err := Seed(db, opts); err != nil {
		t.Fatalf("第一次 Seed: %v", err)
	}
	before := rowCounts(t, db)
	ciphers := keyCiphertexts(t, db)

	// 模拟重启：同样的 seed 再执行一次
	if err := Seed(db, opts); err != nil {
		t.Fatalf("第二次 Seed: %v", err)
	}
	after := rowCounts(t, db)

	want := map[string]int64{
		"groups": 1, "users": 1, "channels": 2, "channel_models": 2,
		"channel_keys": 2, "model_aliases": 1, "model_prices": 1,
	}
	for tbl, n := range want {
		if before[tbl] != n {
			t.Errorf("第一次 seed 后 %s 行数应为 %d，实际 %d", tbl, n, before[tbl])
		}
		if after[tbl] != n {
			t.Errorf("重启后 %s 行数应为 %d，实际 %d（产生重复行）", tbl, n, after[tbl])
		}
	}

	// 密文零重写：AES-GCM nonce 随机，若重加密则字节必变
	afterCiphers := keyCiphertexts(t, db)
	for i := range ciphers {
		if string(ciphers[i]) != string(afterCiphers[i]) {
			t.Errorf("渠道 Key %d 密文在重启后被重写（应为零写）", i)
		}
	}
}

func TestSeed_AdminPasswordChangeConverges(t *testing.T) {
	db := migratedTestDB(t)
	if err := Seed(db, SeedOptions{AdminPassword: "old-pass", MasterKey: testMasterKey()}); err != nil {
		t.Fatalf("Seed(old): %v", err)
	}
	// GAIGATE_ADMIN_PASSWORD 变更后重启：密码应收敛到新值
	if err := Seed(db, SeedOptions{AdminPassword: "new-pass", MasterKey: testMasterKey()}); err != nil {
		t.Fatalf("Seed(new): %v", err)
	}
	var admin User
	if err := db.Where("username = ?", "admin").First(&admin).Error; err != nil {
		t.Fatalf("查询 admin: %v", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(admin.PasswordHash), []byte("new-pass")); err != nil {
		t.Errorf("密码应收敛到 new-pass: %v", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(admin.PasswordHash), []byte("old-pass")); err == nil {
		t.Error("旧密码不应再通过校验")
	}
}

func TestSeed_AfterDownUpChain(t *testing.T) {
	// 11.1 §5 回滚链：down → up 后 schema 与 seed 完整
	opts := testOptions(t)
	recreateTestDatabase(t, opts)
	if err := RunMigrations(opts, "../../migrations"); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := Seed(db, SeedOptions{AdminPassword: "admin-test", MasterKey: testMasterKey()}); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// down → up：迁移需 multiStatements 专用连接（GORM 应用连接不带该参数）
	sqlDB, err := sql.Open("mysql", opts.DSN()+"&multiStatements=true")
	if err != nil {
		t.Fatalf("打开迁移连接: %v", err)
	}
	defer func() { _ = sqlDB.Close() }()
	m, err := newMigrator(sqlDB, "../../migrations")
	if err != nil {
		t.Fatalf("newMigrator: %v", err)
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	// up 后重跑 seed：数据恢复且幂等
	if err := Seed(db, SeedOptions{AdminPassword: "admin-test", MasterKey: testMasterKey()}); err != nil {
		t.Fatalf("回滚链后 Seed: %v", err)
	}
	got := rowCounts(t, db)
	want := map[string]int64{
		"groups": 1, "users": 1, "channels": 2, "channel_models": 2,
		"channel_keys": 2, "model_aliases": 1, "model_prices": 1,
	}
	for tbl, n := range want {
		if got[tbl] != n {
			t.Errorf("回滚链后 %s 行数应为 %d，实际 %d", tbl, n, got[tbl])
		}
	}
}

func TestSeed_InvalidMasterKey(t *testing.T) {
	db := migratedTestDB(t)
	if err := Seed(db, SeedOptions{MasterKey: []byte("short")}); err == nil {
		t.Fatal("master key 非 32 字节应报错")
	}
}

func TestSeed_DefaultAdminPassword(t *testing.T) {
	// GAIGATE_ADMIN_PASSWORD 未设置（空串）时回退 admin123（仅 dev）
	db := migratedTestDB(t)
	if err := Seed(db, SeedOptions{MasterKey: testMasterKey()}); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	var admin User
	if err := db.Where("username = ?", "admin").First(&admin).Error; err != nil {
		t.Fatalf("查询 admin: %v", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(admin.PasswordHash), []byte("admin123")); err != nil {
		t.Errorf("默认密码应为 admin123: %v", err)
	}
}
