-- 000001_init.up：G-AIGate 全量 schema（11.1 §3 字段级冻结 DDL）
-- 11 张表；id 一律 BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY；
-- 时间一律 DATETIME(3) UTC；status 用 TINYINT，含义由 Go 常量定义
-- （internal/store/models.go）。seed 不走 SQL（bcrypt/AES 须运行时计算，
-- 见 11.1 §4 决策①），由 internal/store/seed.go 幂等写入。

CREATE TABLE `groups` (
  `id`         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `name`       VARCHAR(64) NOT NULL,
  `created_at` DATETIME(3) NOT NULL,
  UNIQUE KEY `uk_groups_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `users` (
  `id`            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `username`      VARCHAR(64) NOT NULL,
  `password_hash` VARCHAR(255) NOT NULL,
  `role`          VARCHAR(16) NOT NULL DEFAULT 'user',  -- user|admin
  `group_id`      BIGINT UNSIGNED NOT NULL,
  `status`        TINYINT NOT NULL DEFAULT 1,           -- 1 启用 0 禁用
  `created_at`    DATETIME(3) NOT NULL, `updated_at` DATETIME(3) NOT NULL,
  UNIQUE KEY `uk_users_username` (`username`),
  KEY `idx_users_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `api_keys` (
  `id`         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `user_id`    BIGINT UNSIGNED NOT NULL,
  `name`       VARCHAR(128) NOT NULL DEFAULT '',
  `key_prefix` VARCHAR(16) NOT NULL,                    -- 明文前 8 位，索引用
  `key_hash`   CHAR(64) NOT NULL,                       -- SHA-256 hex
  `status`     TINYINT NOT NULL DEFAULT 1,              -- 1 启用 0 吊销
  `created_at` DATETIME(3) NOT NULL, `expired_at` DATETIME(3) NULL,
  UNIQUE KEY `uk_api_keys_hash` (`key_hash`),
  KEY `idx_api_keys_prefix` (`key_prefix`),
  KEY `idx_api_keys_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `channels` (
  `id`                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `name`              VARCHAR(128) NOT NULL,
  `provider`          VARCHAR(32) NOT NULL,             -- openai|mock|...
  `base_url`          VARCHAR(255) NOT NULL,
  `priority`          INT NOT NULL DEFAULT 0,           -- 大者优先
  `weight`            INT NOT NULL DEFAULT 1,
  `enabled`           TINYINT NOT NULL DEFAULT 1,
  `health_status`     VARCHAR(16) NOT NULL DEFAULT 'unknown', -- 展示缓存列，路由不读
  `error_rate_cache`  FLOAT NOT NULL DEFAULT 0,
  `latency_p99_cache` INT NOT NULL DEFAULT 0,           -- ms，展示缓存列
  `last_checked_at`   DATETIME(3) NULL,
  `created_at`        DATETIME(3) NOT NULL, `updated_at` DATETIME(3) NOT NULL,
  KEY `idx_channels_enabled` (`enabled`, `priority`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `channel_models` (
  `channel_id` BIGINT UNSIGNED NOT NULL,
  `model`      VARCHAR(128) NOT NULL,                   -- 对外模型名（别名）
  `upstream_model` VARCHAR(128) NOT NULL,               -- 映射后的上游模型名
  PRIMARY KEY (`channel_id`, `model`),
  KEY `idx_cm_model` (`model`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- 注意：相对 00 §7.1 增加 upstream_model 列——渠道对同一对外模型可用不同上游名，
-- 这是 channel_models 多对多的本意；属字段级补全，随本 PR 一并登记 00 变更记录。

CREATE TABLE `channel_keys` (
  `id`               BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `channel_id`       BIGINT UNSIGNED NOT NULL,
  `provider_key_enc` VARBINARY(512) NOT NULL,           -- AES-256-GCM 密文
  `weight`           INT NOT NULL DEFAULT 1,
  `status`           TINYINT NOT NULL DEFAULT 1,
  `created_at`       DATETIME(3) NOT NULL,
  KEY `idx_ck_channel` (`channel_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `model_aliases` (
  `id`                 BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `name`               VARCHAR(128) NOT NULL,
  `display_name`       VARCHAR(128) NOT NULL DEFAULT '',
  `default_channel_id` BIGINT UNSIGNED NULL,
  `fallback_ids`       JSON NULL,                       -- 渠道 id 数组，如 [2,3]
  `strategy`           VARCHAR(32) NOT NULL DEFAULT 'weighted',
  `created_at`         DATETIME(3) NOT NULL, `updated_at` DATETIME(3) NOT NULL,
  UNIQUE KEY `uk_alias_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `model_prices` (
  `id`                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `model`             VARCHAR(128) NOT NULL,
  `input_price_per_m`  BIGINT NOT NULL,                 -- 微元 / 百万 token
  `output_price_per_m` BIGINT NOT NULL,
  `currency`          VARCHAR(8) NOT NULL DEFAULT 'CNY',
  `effective_from`    DATETIME(3) NOT NULL,
  `enabled`           TINYINT NOT NULL DEFAULT 1,
  KEY `idx_price_model` (`model`, `effective_from`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `usage_records` (
  `id`            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `request_id`    VARCHAR(32) NOT NULL,
  `user_id`       BIGINT UNSIGNED NOT NULL,
  `api_key_id`    BIGINT UNSIGNED NOT NULL,
  `channel_id`    BIGINT UNSIGNED NOT NULL,
  `model`         VARCHAR(128) NOT NULL,
  `input_tokens`  INT UNSIGNED NOT NULL DEFAULT 0,
  `output_tokens` INT UNSIGNED NOT NULL DEFAULT 0,
  `cost`          BIGINT NOT NULL DEFAULT 0,            -- 微元
  `latency_ms`    INT UNSIGNED NOT NULL DEFAULT 0,
  `status`        VARCHAR(16) NOT NULL,                 -- success|failed|canceled
  `estimated`     TINYINT NOT NULL DEFAULT 0,
  `created_at`    DATETIME(3) NOT NULL,
  KEY `idx_usage_req` (`request_id`),
  KEY `idx_usage_user_time` (`user_id`, `created_at`),
  KEY `idx_usage_time` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `quotas` (
  `id`          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `target_type` VARCHAR(16) NOT NULL,                   -- api_key|user|group
  `target_id`   BIGINT UNSIGNED NOT NULL,
  `period`      VARCHAR(16) NOT NULL,                   -- daily|monthly
  `token_limit` BIGINT NOT NULL DEFAULT 0,              -- 0 = 不限
  `cost_limit`  BIGINT NOT NULL DEFAULT 0,              -- 微元，0 = 不限
  `soft_ratio`  DECIMAL(3,2) NOT NULL DEFAULT 0.80,
  UNIQUE KEY `uk_quota_target` (`target_type`, `target_id`, `period`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `bills` (
  `id`          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  `user_id`     BIGINT UNSIGNED NOT NULL,
  `group_id`    BIGINT UNSIGNED NOT NULL,
  `period`      CHAR(7) NOT NULL,                       -- 'YYYY-MM'
  `total_cost`  BIGINT NOT NULL DEFAULT 0,
  `token_total` BIGINT NOT NULL DEFAULT 0,
  `created_at`  DATETIME(3) NOT NULL,
  UNIQUE KEY `uk_bills_user_period` (`user_id`, `period`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
