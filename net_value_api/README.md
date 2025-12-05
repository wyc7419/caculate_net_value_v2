# 净值数据 API 接口文档

供第三方系统调用的净值数据接口。

## 📥 导入到 Apifox

本项目提供了 OpenAPI 3.0 规范文档，可以直接导入到 Apifox：

### 快速导入（推荐）

1. 打开 Apifox 客户端
2. 选择项目 → 「项目设置」→「数据导入」
3. 选择「OpenAPI/Swagger」
4. 上传文件：`net_value_api/openapi.yaml`
5. 选择「智能合并」模式
6. 完成导入 ✅

### 详细教程

查看完整导入指南：[APIFOX_IMPORT_GUIDE.md](./APIFOX_IMPORT_GUIDE.md)

包含：
- 📄 文件导入方法
- 🔗 URL 自动同步
- 🧪 接口测试配置
- 👥 团队协作设置

---

## 启动服务

```bash
# 默认端口 8080
python net_value_api/server.py

# 指定端口
python net_value_api/server.py --port 9000

# 指定监听地址
python net_value_api/server.py --host 127.0.0.1 --port 8080
```

## 接口列表

### 1. 查询可用时间周期

**请求**
```
GET /netvalue/intervals
```

**响应**
```json
{
    "success": true,
    "data": {
        "intervals": ["1h", "2h", "4h", "8h", "12h", "1d"],
        "descriptions": {
            "1h": "1小时",
            "2h": "2小时",
            "4h": "4小时",
            "8h": "8小时",
            "12h": "12小时",
            "1d": "1天"
        }
    }
}
```

---

### 2. 查询净值数据

**请求**
```
GET /netvalue/data/<interval>/<address>?fields=<fields>
```

**路径参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| interval | string | 时间周期（1h, 2h, 4h, 8h, 12h, 1d） |
| address | string | 账户地址（0x开头的42位字符串） |

**查询参数**
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| fields | string | 否 | all | 要导出的字段，逗号分隔 |
| from_first_trade | bool | 否 | true | 是否从第一笔交易开始返回数据 |
| normalize | bool | 否 | true | 是否归一化处理（分页时自动禁用） |
| page | int | 否 | - | 页码（从1开始，指定后启用分页） |
| page_size | int | 否 | 1000 | 每页记录数（1-5000） |

**fields 可选值**
- `all`：导出全部字段（默认）
- 单个字段：`net_value`
- 多个字段：`net_value,cumulative_pnl,total_assets`

**from_first_trade 说明**
- `true`（默认）：只返回从第一笔交易所在时间区间开始的数据，过滤掉之前的空数据
- `false`：返回所有数据（包括第一笔交易前的数据）

**normalize 说明**
- `true`（默认）：将 net_value 归一化处理，以第一条记录的净值为基准（=1.0）
- `false`：返回原始净值
- 仅当 `from_first_trade=true` 且未分页时有效
- 归一化后响应中会包含 `base_net_value`（原始基准净值）
- **注意**：使用分页时自动禁用归一化

**分页说明**
- 指定 `page` 参数后启用分页模式
- `page` 从 1 开始
- `page_size` 默认 1000，最大 5000
- 分页时响应中包含 `pagination` 对象

**可用字段列表**

> 注意：`timestamp` 始终导出，不需要指定

| 字段 | 说明 |
|------|------|
| net_value | 净值 |
| cumulative_pnl | 累计盈亏（美元） |
| total_assets | 总资产（美元） |
| total_shares | 总份额 |
| spot_account_value | 现货账户价值（美元） |
| perp_account_value | 合约账户价值（美元） |
| realized_pnl | 已实现盈亏（美元） |
| virtual_pnl | 虚拟盈亏（美元） |

**响应示例（fields=all 或不指定）**
```json
{
    "success": true,
    "data": {
        "address": "0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4",
        "interval": "1h",
        "fields": ["net_value", "cumulative_pnl", "total_assets", "total_shares", "spot_account_value", "perp_account_value", "realized_pnl", "virtual_pnl"],
        "first_trade_timestamp": 1704067200000,
        "from_first_trade": true,
        "records": [
            {
                "timestamp": 1704067200000,
                "net_value": 1.0523,
                "cumulative_pnl": 1523.45,
                "total_assets": 102345.67,
                "total_shares": 10000.0,
                "spot_account_value": 50000.0,
                "perp_account_value": 52345.67,
                "realized_pnl": 800.0,
                "virtual_pnl": 723.45
            }
        ],
        "stats": {
            "total_records": 100,
            "start_time": 1704067200000,
            "end_time": 1704153600000,
            "first_net_value": 1.0,
            "last_net_value": 1.0523,
            "return_rate": 5.23,
            "first_pnl": 0,
            "last_pnl": 1523.45
        }
    }
}
```

**响应示例（from_first_trade=false）**
```json
{
    "success": true,
    "data": {
        "address": "0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4",
        "interval": "1h",
        "fields": ["net_value", "cumulative_pnl"],
        "first_trade_timestamp": 1704067200000,
        "from_first_trade": false,
        "normalize": false,
        "records": [...],
        "stats": {...}
    }
}
```

**响应示例（normalize=true）**
```json
{
    "success": true,
    "data": {
        "address": "0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4",
        "interval": "1h",
        "fields": ["net_value"],
        "first_trade_timestamp": 1704067200000,
        "from_first_trade": true,
        "normalize": true,
        "records": [
            {"timestamp": 1704067200000, "net_value": 1.0},
            {"timestamp": 1704070800000, "net_value": 1.0523},
            {"timestamp": 1704074400000, "net_value": 1.0812}
        ],
        "stats": {
            "total_records": 3,
            "start_time": 1704067200000,
            "end_time": 1704074400000,
            "first_net_value": 1.0,
            "last_net_value": 1.0812,
            "return_rate": 8.12,
            "base_net_value": 102345.67
        }
    }
}
```

**响应示例（分页模式）**
```json
{
    "success": true,
    "data": {
        "address": "0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4",
        "interval": "1h",
        "fields": ["net_value"],
        "first_trade_timestamp": 1704067200000,
        "from_first_trade": true,
        "normalize": false,
        "records": [...],
        "stats": {...},
        "pagination": {
            "page": 1,
            "page_size": 1000,
            "total_records": 8760,
            "total_pages": 9,
            "has_next": true,
            "has_prev": false
        }
    }
}
```

**响应（失败）**
```json
{
    "success": false,
    "error": "未找到该地址的净值数据"
}
```

---

## 错误码

| HTTP 状态码 | 说明 |
|-------------|------|
| 200 | 成功 |
| 400 | 请求参数错误（无效的时间周期或地址格式） |
| 404 | 未找到数据 |
| 500 | 服务器内部错误 |

---

## 示例

**cURL**
```bash
# 查询可用周期
curl http://localhost:8080/netvalue/intervals

# 查询净值数据（全部字段）
curl http://localhost:8080/netvalue/data/1h/0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4

# 查询净值数据（只要 net_value）
curl "http://localhost:8080/netvalue/data/1h/0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4?fields=net_value"

# 查询净值数据（多个字段，timestamp 自动包含）
curl "http://localhost:8080/netvalue/data/1h/0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4?fields=net_value,cumulative_pnl"

# 查询净值数据（返回全部数据，包括第一笔交易前的数据）
curl "http://localhost:8080/netvalue/data/1h/0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4?from_first_trade=false"

# 查询净值数据（分页，第1页，每页1000条）
curl "http://localhost:8080/netvalue/data/1h/0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4?page=1&page_size=1000"

# 查询净值数据（分页，第2页）
curl "http://localhost:8080/netvalue/data/1h/0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4?page=2"
```

**Python**
```python
import requests

# 查询可用周期
resp = requests.get('http://localhost:8080/netvalue/intervals')
print(resp.json())

# 查询净值数据（全部字段）
address = '0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4'
resp = requests.get(f'http://localhost:8080/netvalue/data/1h/{address}')
data = resp.json()

if data['success']:
    for record in data['data']['records']:
        print(f"{record['timestamp']}: 净值={record['net_value']:.4f}")

# 查询净值数据（指定字段）
resp = requests.get(
    f'http://localhost:8080/netvalue/data/1h/{address}',
    params={'fields': 'net_value,cumulative_pnl'}
)
data = resp.json()

if data['success']:
    print(f"导出字段: {data['data']['fields']}")
    for record in data['data']['records']:
        print(record)
```

**JavaScript**
```javascript
// 查询可用周期
fetch('http://localhost:8080/netvalue/intervals')
    .then(r => r.json())
    .then(data => console.log(data));

// 查询净值数据（全部字段）
const address = '0x48360dff8fa1ef91b820d014ecb6eb40ea4f3cd4';
fetch(`http://localhost:8080/netvalue/data/1h/${address}`)
    .then(r => r.json())
    .then(data => {
        if (data.success) {
            data.data.records.forEach(r => {
                console.log(`${r.timestamp}: 净值=${r.net_value.toFixed(4)}`);
            });
        }
    });

// 查询净值数据（指定字段）
fetch(`http://localhost:8080/netvalue/data/1h/${address}?fields=net_value,cumulative_pnl`)
    .then(r => r.json())
    .then(data => {
        if (data.success) {
            console.log('导出字段:', data.data.fields);
            data.data.records.forEach(r => console.log(r));
        }
    });
```

