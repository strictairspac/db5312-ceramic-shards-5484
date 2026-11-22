# 初始结构

`src/models/tables.py` 定义初始关系结构。API 启动时显式创建演示库的表；此版本只包含初始 schema。需要在已有部署升级表结构时，应单独引入版本化迁移。

## 可恢复试算（断点续跑）

已有部署升级到断点续跑版本时，需为 `business_runs` 补充执行计划、租约与进度字段，并新建 `business_checkpoints` 表。`create_all` 只建缺失的表，不会改已有表结构，因此旧库必须手工迁移：

```sql
ALTER TABLE business_runs
  ADD COLUMN lease_expires_at VARCHAR(32) NULL,
  ADD COLUMN plan JSON NULL,
  ADD COLUMN progress JSON NULL;

CREATE TABLE business_checkpoints (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  run_id INTEGER NOT NULL,
  step_index INTEGER NOT NULL,
  batch_id VARCHAR(64) NOT NULL,
  source_node VARCHAR(64) NOT NULL,
  target_node VARCHAR(64) NOT NULL,
  size_bytes INTEGER NOT NULL,
  attempt_token VARCHAR(36) NOT NULL,
  confirmed_at VARCHAR(32) NOT NULL,
  CONSTRAINT uq_checkpoint_run_step UNIQUE (run_id, step_index),
  CONSTRAINT fk_checkpoint_run FOREIGN KEY (run_id) REFERENCES business_runs (id),
  INDEX ix_business_checkpoints_run_id (run_id)
);
```

旧版本遗留的 `running` 运行没有 `lease_expires_at`，会被视为租约已过期，由 worker 接手续跑。
