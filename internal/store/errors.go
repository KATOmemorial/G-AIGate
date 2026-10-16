package store

import "errors"

// ErrNotFound 表示目标配置行不存在（Get/Update/Delete/Resolve 未命中）。
// 跨模块哨兵：消费方以 errors.Is 判定并映射为 404（如 14.1 管理 API）。
// TODO(00): 00 §6.7 补登 ErrNotFound 跨模块哨兵（见 docs/spec/02-遗留事项台账.md #6）。
var ErrNotFound = errors.New("store: 记录不存在")

// ErrNoPrice 表示指定模型在给定时刻没有 enabled 且已生效（effective_from<=at）
// 的价格（11.2 §3 冻结语义）。降级策略由 17.3 决定，store 不做业务判断。
// TODO(00): 00 §6.7 补登 ErrNoPrice 跨模块哨兵（见 docs/spec/02-遗留事项台账.md #6）。
var ErrNoPrice = errors.New("store: 模型无生效价格")
