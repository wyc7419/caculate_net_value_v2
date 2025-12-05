# 净值数据库架构

## 数据库选型

**TimescaleDB** (PostgreSQL 扩展)
- 时序数据库，专为时间序列数据优化
- 支持自动分区（Hypertable）
- 内置数据压缩和保留策略
- 完全兼容 SQL

---

## 表结构设计

### 1. 净值数据表（按周期分表）

```
net_value_1h    (1小时周期)
net_value_2h    (2小时周期)
net_value_4h    (4小时周期)
net_value_8h    (8小时周期)
net_value_12h   (12小时周期)
net_value_1d    (1天周期)
```

**字段定义：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `address` | TEXT | 账户地址 |
| `time` | TIMESTAMPTZ | 时间戳（分区键） |
| `spot_account_value` | DOUBLE PRECISION | 现货账户价值 |
| `realized_pnl` | DOUBLE PRECISION | 已实现盈亏 |
| `virtual_pnl` | DOUBLE PRECISION | 虚拟盈亏 |
| `perp_account_value` | DOUBLE PRECISION | 合约账户价值 |
| `total_assets` | DOUBLE PRECISION | 总资产 |
| `total_shares` | DOUBLE PRECISION | 总份额 |
| `net_value` | DOUBLE PRECISION | 净值 |
| `cumulative_pnl` | DOUBLE PRECISION | 累计盈亏 |

### 2. 更新记录表

```sql
net_value_update_records
```

**字段定义：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `address` | TEXT | 账户地址（主键） |
| `first_trade_timestamp` | BIGINT | 第一笔交易时间戳（毫秒） |
| `time_1h` | TIMESTAMPTZ | 1h 数据最后更新时间 |
| `time_2h` | TIMESTAMPTZ | 2h 数据最后更新时间 |
| `time_4h` | TIMESTAMPTZ | 4h 数据最后更新时间 |
| `time_8h` | TIMESTAMPTZ | 8h 数据最后更新时间 |
| `time_12h` | TIMESTAMPTZ | 12h 数据最后更新时间 |
| `time_1d` | TIMESTAMPTZ | 1d 数据最后更新时间 |

---

## TimescaleDB 配置

### 1. Hypertable（超表）

**分区键**: `time`（时间戳）

**分块间隔**（根据周期自动设置）：

| 周期 | 分块间隔 | 每块记录数 |
|------|---------|-----------|
| 1h | 1 month | ~720 |
| 2h | 2 months | ~720 |
| 4h | 3 months | ~540 |
| 8h | 6 months | ~540 |
| 12h | 1 year | ~730 |
| 1d | 1 year | ~365 |

```sql
SELECT create_hypertable(
    'net_value_1h', 
    'time',
    chunk_time_interval => INTERVAL '1 month'
);
```

### 2. 索引策略

**唯一索引**（防重复）：
```sql
CREATE UNIQUE INDEX net_value_1h_address_time_idx 
ON net_value_1h (address, time DESC);
```

**地址索引**（加速查询）：
```sql
CREATE INDEX net_value_1h_address_idx 
ON net_value_1h (address);
```

### 3. 压缩策略

**7天后自动压缩**（节省存储空间）：

```sql
ALTER TABLE net_value_1h SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'address'
);

SELECT add_compression_policy('net_value_1h', INTERVAL '7 days');
```

---

## 数据写入流程

```
计算净值数据
    ↓
检查地址是否存在于 update_records
    ↓
├─ 存在: 增量更新（追加新数据）
│       INSERT INTO net_value_1h ... 
│       ON CONFLICT (address, time) DO UPDATE ...
│       
└─ 不存在: 全量插入
        INSERT INTO net_value_1h ...
        
更新 update_records
    ↓
UPDATE net_value_update_records 
SET time_1h = NOW(), first_trade_timestamp = ...
```

---

## 数据查询优化

### 1. 按地址查询（最常见）
```sql
SELECT * FROM net_value_1h 
WHERE address = '0x123...' 
ORDER BY time DESC;
```
- 使用 `address_idx` 索引
- 自动分区剪枝

### 2. 按时间范围查询
```sql
SELECT * FROM net_value_1h 
WHERE address = '0x123...' 
  AND time >= '2025-01-01' 
  AND time < '2025-12-31'
ORDER BY time DESC;
```
- 分区剪枝（只扫描相关分区）
- 压缩块自动解压

### 3. 分页查询
```sql
SELECT * FROM net_value_1h 
WHERE address = '0x123...' 
ORDER BY time DESC 
LIMIT 1000 OFFSET 0;
```

---

## 维护策略

### 1. 自动压缩
- **触发时间**: 数据写入 7 天后
- **压缩方式**: 按 `address` 分组压缩
- **压缩率**: 约 90%（取决于数据重复度）

### 2. 手动压缩（可选）
```sql
SELECT compress_chunk(i) FROM show_chunks('net_value_1h') i;
```

### 3. 查看分区状态
```sql
SELECT * FROM timescaledb_information.chunks 
WHERE hypertable_name = 'net_value_1h';
```

### 4. 清理旧数据（可选）
```sql
-- 添加保留策略（2年）
SELECT add_retention_policy('net_value_1h', INTERVAL '2 years');

-- 手动删除旧数据
DELETE FROM net_value_1h WHERE time < NOW() - INTERVAL '2 years';
```

---

## 性能指标

| 操作 | 性能 |
|------|------|
| 单地址查询（1年数据） | < 100ms |
| 插入 1000 条记录 | < 50ms |
| 压缩后存储空间 | 减少 90% |
| 并发查询支持 | 1000+ QPS |

---

## Docker 部署配置

**docker-compose.yml**:
```yaml
timescaledb:
  image: timescale/timescaledb:latest-pg15
  environment:
    - POSTGRES_DB=tsdb
    - POSTGRES_USER=postgres
    - POSTGRES_PASSWORD=password
  volumes:
    - timescaledb_data:/var/lib/postgresql/data
  ports:
    - "5432:5432"
```

**环境变量**:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tsdb
DB_USER=postgres
DB_PASSWORD=password
```

---

## 故障排查

### 1. 连接失败
```bash
# 检查 TimescaleDB 是否运行
docker compose ps

# 查看日志
docker compose logs timescaledb
```

### 2. 表不存在
```bash
# 首次使用会自动创建表
# 手动创建：
python -c "from main.net_value_timescale_manager import NetValueTimescaleManager; 
           db = NetValueTimescaleManager(); 
           db._create_hypertable_if_not_exists(db._get_connection(), 'net_value_1h', '1h')"
```

### 3. 数据重复
```bash
# 唯一索引会自动防止重复
# 如发现重复，检查 (address, time) 组合
SELECT address, time, COUNT(*) 
FROM net_value_1h 
GROUP BY address, time 
HAVING COUNT(*) > 1;
```

