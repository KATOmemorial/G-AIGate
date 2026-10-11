-- 000001_init.down：完整回滚 000001_init（11.1 §5 验收的 down→up 回滚链）。
-- 表间无外键（DDL 未定义 FK），逆序 DROP 仅保持与 up 相反的可读顺序。
-- schema_migrations 由 golang-migrate 管理，不在本文件职责内。

DROP TABLE IF EXISTS `bills`;
DROP TABLE IF EXISTS `quotas`;
DROP TABLE IF EXISTS `usage_records`;
DROP TABLE IF EXISTS `model_prices`;
DROP TABLE IF EXISTS `model_aliases`;
DROP TABLE IF EXISTS `channel_keys`;
DROP TABLE IF EXISTS `channel_models`;
DROP TABLE IF EXISTS `channels`;
DROP TABLE IF EXISTS `api_keys`;
DROP TABLE IF EXISTS `users`;
DROP TABLE IF EXISTS `groups`;
