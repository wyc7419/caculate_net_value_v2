# 事件影响记录器 (EventImpactRecorder) 处理逻辑详解

## 一、概述

事件影响记录器是一个用于记录和分析每个交易事件对账户影响的工具类，适用于基于份额法的净值曲线计算。

### 核心概念

| 概念 | 说明 |
|------|------|
| 持仓变化 | 记录币种数量的变化。合约：正数=多头，负数=空头 |
| 除持仓影响的资产变化 | 分为现货资产变化和合约资产变化，不含持仓价值变动 |
| 份额变化 | 只有充值、提现、外部转账等操作影响份额 |

### 支持的事件类型

- **Trade（交易）**: perp（合约交易）、spot（现货交易）
- **Funding（资金费）**: funding
- **Ledger（账本）**: deposit、withdraw、accountClassTransfer、spotTransfer、internalTransfer、subAccountTransfer、vaultCreate、vaultDeposit、vaultWithdraw、vaultDistribution、spotGenesis、send、cStakingTransfer、liquidation、rewardsClaim、activateDexAbstraction、accountActivationGas

---

## 二、初始化流程

### 2.1 构造函数执行顺序

1. 将账户地址转换为小写存储
2. 初始化数据存储变量：`data`、`timeline`、`impacts`
3. 如果提供了地址，依次执行：
   - `load_data()` - 加载数据
   - `extract_account_address()` - 提取地址
   - `build_timeline()` - 构建时间线

### 2.2 数据加载 (load_data)

通过 `DataLoader` 从 API 加载三类数据：
- 交易数据 (trades)
- 资金费数据 (funding)
- 账本数据 (ledger)

返回格式：
```
{
    'address': '0x...',
    'data': {
        'trade': [...],
        'funding': [...],
        'ledger': {'data': [...]}
    }
}
```

### 2.3 构建时间线 (build_timeline)

#### 处理交易数据 (trades)
- 深拷贝每条交易记录
- 处理 coin 字段：去掉 `/USDC` 后缀
- 添加到 timeline，设置 `type='trade'`，`subtype` 取自交易的 `type` 字段

#### 处理资金费数据 (funding)
- 直接添加到 timeline，设置 `type='funding'`

#### 处理账本数据 (ledger)
- 从 `ledger.data` 中提取记录
- 从 `delta.type` 获取子类型
- 添加到 timeline，设置 `type='ledger'`

#### 排序规则
- 按时间**倒序**排列（从新到旧）
- 排序字段：`time`（毫秒时间戳）

---

## 三、事件处理流程

### 3.1 处理所有事件 (process_all_events)

遍历 timeline 中的每个事件：
1. 调用 `record_event_impact()` 获取影响记录
2. 添加元信息：
   - `event_number`：事件序号（从1开始）
   - `event_time`：原始毫秒时间戳
   - `event_time_str`：北京时间字符串格式
3. 每处理 10000 个事件打印进度

### 3.2 事件分发 (record_event_impact)

根据 `event['type']` 分发到对应处理方法：
- `trade` → `record_trade_impact()`
- `funding` → `record_funding_impact()`
- `ledger` → `record_ledger_impact()`
- 其他 → `_empty_impact()`

---

## 四、交易事件处理详解

### 4.1 交易类型判断

根据 `event['subtype']` 判断：
- `perp` 或 `perps` → 合约交易
- `spot` → 现货交易

### 4.2 合约交易处理 (record_perp_trade_impact)

#### 输入字段解析

| 字段 | 类型 | 说明 |
|------|------|------|
| coin | str | 币种名称 |
| side | str | 'B'=买入, 'A'=卖出 |
| sz | float | 交易数量 |
| px | float | 交易价格 |
| startPosition | float | 交易前持仓 |
| closedPnl | float | 已实现盈亏 |
| fee | float | 手续费 |
| feeToken | str | 手续费代币 |
| dir | str | 交易方向描述 |

#### 手续费处理逻辑

- **feeToken == 'USDC'**: 
  - `perp_asset_change_ex_position = -fee`
- **feeToken != 'USDC'**: 
  - 打印警告，手续费不记录
  - `perp_asset_change_ex_position = 0`

#### side 验证
- 必须是 'B' 或 'A'，否则抛出 ValueError

#### 交易后持仓计算
- side='B'（买入）：`endPosition = startPosition + sz`
- side='A'（卖出）：`endPosition = startPosition - sz`

#### 交易前持仓记录 (before_perp_trade)
根据 startPosition 判断方向：
- startPosition > 0 → dir='long'
- startPosition < 0 → dir='short'
- startPosition = 0 → dir=''

格式：
```
{
    'coin': 'BTC',
    'amount': startPosition,
    'dir': 'long'/'short'/''
}
```

#### 持仓变化记录 (perp_position_changes)
```
{
    coin: {
        'amount': sz,        # 交易数量
        'price': px,         # 交易价格
        'dir': trade_dir,    # 交易方向（如 'Open Long', 'Close Short'）
        'side': side         # 'B' 或 'A'
    }
}
```

#### 输出结构
- `spot_position_changes`: {}（空）
- `perp_position_changes`: 包含交易信息
- `spot_asset_change_ex_position`: 0
- `perp_asset_change_ex_position`: -fee 或 0
- `share_change`: 0（交易不影响份额）

### 4.3 现货交易处理 (record_spot_trade_impact)

#### 输入字段解析

| 字段 | 类型 | 说明 |
|------|------|------|
| coin | str | 交易币种 |
| side | str | 'B'=买入, 'A'=卖出 |
| sz | float | 交易数量 |
| px | float | 交易价格 |
| fee | float | 手续费 |
| feeToken | str | 手续费代币 |

#### 交易变化计算

**买入 (side='B')**:
- `coin_change = +sz`（币增加）
- `usdc_change = -(sz × px)`（USDC减少）

**卖出 (side='A')**:
- `coin_change = -sz`（币减少）
- `usdc_change = +(sz × px)`（USDC增加）

#### 手续费处理（三种情况）

**情况1: feeToken == 'USDC'**
- 手续费影响资产，不影响币种持仓
- `spot_position_changes[coin] = {'change': coin_change}`
- `spot_position_changes['USDC'] = {'change': usdc_change}`
- `spot_asset_change_ex_position = -fee`

**情况2: feeToken == coin（交易币种）**
- 手续费从该币种扣除
- `spot_position_changes[coin] = {'change': coin_change - fee}`
- `spot_position_changes['USDC'] = {'change': usdc_change}`
- `spot_asset_change_ex_position = 0`

**情况3: feeToken 是其他币种**
- 币种持仓变化不受手续费影响
- `spot_position_changes[coin] = {'change': coin_change}`
- `spot_position_changes['USDC'] = {'change': usdc_change}`
- `spot_position_changes[feeToken] = {'change': -fee}`
- `spot_asset_change_ex_position = 0`

---

## 五、资金费处理详解 (record_funding_impact)

### 输入解析
- 从 `delta.usdc` 获取资金费金额

### 处理逻辑
- 资金费直接影响合约资产
- usdc 为负数表示支付，正数表示收到
- `perp_asset_change_ex_position = usdc_change`

### 影响输出
- `spot_position_changes`: {}（空）
- `perp_position_changes`: {}（空，资金费不影响持仓）
- `spot_asset_change_ex_position`: 0
- `perp_asset_change_ex_position`: usdc_change
- `share_change`: 0

---

## 六、账本事件处理详解 (record_ledger_impact)

### 6.1 事件分发

根据 `event['subtype']` 分发到对应方法。对于 `spotGenesis` 和 `rewardsClaim`，需要额外传递完整事件数据用于获取价格。

### 6.2 充值 (deposit)

**处理逻辑**:
- 充值增加合约账户的 USDC
- `perp_asset_change_ex_position = +usdc`
- `share_change = "usdc/current_net_value"`（字符串公式）

### 6.3 提现 (withdraw)

**处理逻辑**:
- 提现减少合约账户的 USDC，需扣除手续费
- `perp_asset_change_ex_position = -(usdc + fee)`
- `share_change = "-usdc/current_net_value"`

**注意**: 份额变化只计 usdc，不计 fee

### 6.4 账户类别转账 (accountClassTransfer)

用于 perp 和 spot 之间的 USDC 转账。

**toPerp=True (Spot → Perp)**:
- `perp_asset_change_ex_position = +usdc`
- `spot_position_changes['USDC'] = {'change': -usdc}`

**toPerp=False (Perp → Spot)**:
- `perp_asset_change_ex_position = -usdc`
- `spot_position_changes['USDC'] = {'change': +usdc}`

**share_change**: 0（内部转账不影响份额）

### 6.5 现货转账 (spotTransfer)

**转出 (user == 本账户)**:
- `spot_position_changes[token] = {'change': -amount}`
- `spot_asset_change_ex_position = -fee`
- `share_change = "-(usdcValue+fee)/current_net_value"`

**转入 (destination == 本账户)**:
- `spot_position_changes[token] = {'change': +amount}`
- `spot_asset_change_ex_position = 0`
- `share_change = "usdcValue/current_net_value"`

### 6.6 内部转账 (internalTransfer)

仅限 USDC 转账。

**转出 (user == 本账户)**:
- `spot_position_changes['USDC'] = {'change': -usdc}`
- `spot_asset_change_ex_position = -fee`
- `share_change = "-(usdc+fee)/current_net_value"`

**转入 (destination == 本账户)**:
- `spot_position_changes['USDC'] = {'change': +usdc}`
- `share_change = "usdc/current_net_value"`

### 6.7 子账户转账 (subAccountTransfer)

**转出 (user == 本账户)**:
- `spot_position_changes['USDC'] = {'change': -usdc}`
- `share_change = "-usdc/current_net_value"`

**转入 (destination == 本账户)**:
- `spot_position_changes['USDC'] = {'change': +usdc}`
- `share_change = "usdc/current_net_value"`

### 6.8 金库存款 (vaultDeposit)

- `spot_position_changes['USDC'] = {'change': -usdc}`
- `share_change = "-usdc/current_net_value"`

### 6.9 金库取款 (vaultWithdraw)

- `spot_position_changes['USDC'] = {'change': +netWithdrawnUsd}`
- `share_change = "netWithdrawnUsd/current_net_value"`

### 6.10 金库收益分配 (vaultDistribution)

- `spot_position_changes['USDC'] = {'change': +usdc}`
- `share_change = "usdc/current_net_value"`
- `spot_asset_change_ex_position = 0`（影响已体现在持仓变化中）

### 6.11 创建金库 (vaultCreate)

总成本 = usdc（初始资金）+ fee（创建费用）

- `spot_position_changes['USDC'] = {'change': -(usdc + fee)}`
- `share_change = "-(usdc+fee)/current_net_value"`

### 6.12 现货空投 (spotGenesis)

- `spot_position_changes[token] = {'change': +amount}`

**份额变化计算**:
- 如果能获取价格：`share_change = "(amount × price)/current_net_value"`
- 如果无法获取价格：`share_change = 0`

**价格获取**: 使用 `kline_fetcher.get_open_price()` 获取现货价格，参数 `interval='auto'` 自动选择最佳精度。

### 6.13 Send 事件 (send) - 复杂转账

处理6种情况：

| 情况 | 条件 | 处理 |
|------|------|------|
| 1. Perp → Spot（内部） | user=dest=本账户, sourceDex='', destDex='spot' | spot: +amount, perp: -(fee+amount), share: 0 |
| 2. Spot → Perp（内部） | user=dest=本账户, sourceDex='spot', destDex='' | spot: -amount, perp: +amount, spot_fee: -fee, share: 0 |
| 3. Perp 转出外部 | user=本账户, sourceDex='', dest≠本账户或destDex外部 | perp: -(amount+fee), share: 负值 |
| 4. Spot 转出外部 | user=本账户, sourceDex='spot', dest≠本账户或destDex外部 | spot: -amount, spot_fee: -fee, share: 负值 |
| 5. 外部转入 Perp | dest=本账户, destDex='', user≠本账户或sourceDex外部 | perp: +amount, share: 正值 |
| 6. 外部转入 Spot | dest=本账户, destDex='spot', user≠本账户或sourceDex外部 | spot: +amount, share: 正值 |

**注意**: 情况3和5要求 token 必须是 USDC，否则抛出错误。

### 6.14 质押转账 (cStakingTransfer)

- 所有变化均为 0
- 质押只是状态转换，不影响总资产

### 6.15 清算 (liquidation)

- 所有变化均为 0
- 实际交易细节在 trades 中记录

### 6.16 奖励领取 (rewardsClaim)

**USDC 奖励 (token=='' 或 token=='USDC')**:
- `spot_position_changes['USDC'] = {'change': +amount}`
- `share_change = "amount/current_net_value"`

**其他代币奖励**:
- `spot_position_changes[token] = {'change': +amount}`
- 需要获取价格计算份额变化
- `share_change = "(amount × price)/current_net_value"`

### 6.17 DEX 抽象层激活 (activateDexAbstraction)

- 所有变化均为 0
- 这是授权操作，类似 ERC20 的 approve

### 6.18 账户激活 Gas (accountActivationGas)

- `spot_position_changes[token] = {'change': -amount}`
- 其他变化均为 0
- Gas 费用不影响份额

---

## 七、数值精度处理

### 7.1 数值格式化 (_format_number)

处理流程：
1. 检查空值：如果值为空字符串或 None，返回空字符串
2. 转换为浮点数
3. 如果值为 0，返回 '0'
4. 使用 `:.10f` 格式化（保留10位小数）
5. 去除末尾的 '0'
6. 去除末尾的 '.'（如果有）

示例：
- `123.45600000` → `'123.456'`
- `100.00000000` → `'100'`
- `0.00000001` → `'0.00000001'`

### 7.2 持仓字典格式化 (_format_position_dict)

支持三种输入格式：

**格式1: 合约交易新格式**
```
{'BTC': {'amount': 5, 'price': 50000, 'dir': 'Open Long', 'side': 'B'}}
```
输出: `{'BTC': 'amount': 5, 'price': 50000, 'dir': Open Long}`

**格式2: 原操作格式**
```
{'BTC': {'change': 100}}
```
输出: `{'BTC': 100}`

**格式3: 数字值**
```
{'BTC': 100}
```
输出: `{'BTC': 100}`

### 7.3 交易前持仓格式化 (_format_before_trade)

**现货格式**:
```
{'coin': 'BTC', 'amount': 1.5}
```

**合约格式**:
```
{'coin': 'BTC', 'amount': 410.85977, 'dir': 'long'}
```

---

## 八、时间处理

### 8.1 时间戳转北京时间 (_to_beijing_time)

处理流程：
1. 将毫秒时间戳除以1000转换为秒
2. 使用 `datetime.utcfromtimestamp()` 转换为 UTC 时间
3. 加 8 小时转换为北京时间（UTC+8）

### 8.2 时间字符串格式化 (_to_beijing_time_str)

格式：`%Y-%m-%d %H:%M:%S`

示例：`2025-11-15 14:30:00`

---

## 九、CSV 导出

### 9.1 导出字段

| 字段名 | 说明 |
|--------|------|
| 事件序号 | 从1开始的序号 |
| 时间 | 北京时间字符串 |
| 时间戳 | 毫秒时间戳（int64） |
| 事件大类 | trade/funding/ledger |
| 事件类型 | 具体类型 |
| oid | 订单ID |
| sz | 交易数量 |
| fee | 手续费 |
| feeToken | 手续费代币 |
| closedPnl | 已实现盈亏 |
| startPosition | 起始持仓 |
| usdc | USDC金额 |
| usdcValue | USDC价值 |
| toPerp | 是否转向合约 |
| user | 转出地址 |
| destination | 转入地址 |
| 现货持仓变化 | 格式化的字典 |
| 合约持仓变化 | 格式化的字典 |
| 除持仓影响的现货资产变化 | 数值 |
| 除持仓影响的合约资产变化 | 数值 |
| 总份额变化 | 字符串公式 |

### 9.2 导出配置

- 编码：`utf-8-sig`（支持Excel直接打开中文）
- 时间戳：转为 `int64` 避免科学计数法
- 引用模式：`QUOTE_NONNUMERIC`（确保字符串正确引用）
- 浮点精度：`float_format='%.8f'`

---

## 十、开仓判断逻辑 (is_perp_open_trade)

双重判断机制：

**第一道防线**：检查 closedPnl
- 如果 closedPnl ≠ 0，一定不是开仓（是平仓）

**第二道防线**：检查 dir 字段
- dir 为 'Open Long' 或 'Open Short' 表示开仓

---

## 十一、错误处理

### 11.1 抛出 ValueError 的情况

1. 合约/现货交易的 side 参数不是 'B' 或 'A'
2. Send 事件的情况3/5中 token 不是 USDC
3. Send 事件不匹配任何已知情况
4. 遇到未知的 ledger 类型

### 11.2 打印警告的情况

1. 合约交易的 feeToken 不是 USDC
2. 无法获取空投/奖励币种的价格

---

## 十二、影响记录标准结构

```python
{
    'event_type': str,           # 事件大类
    'event_subtype': str,        # 事件子类型
    'coin': str,                 # 相关币种（可选）
    
    # 交易前持仓（仅交易事件有值）
    'before_spot_trade': {},
    'before_perp_trade': {},
    
    # 持仓变化
    'spot_position_changes': {},
    'perp_position_changes': {},
    
    # 资产变化（除持仓影响）
    'spot_asset_change_ex_position': float,
    'perp_asset_change_ex_position': float,
    
    # 份额变化
    'share_change': str or int,  # 字符串公式或0
    
    # 原始数据
    'raw_data': {}
}
```

---

## 附录：事件影响汇总表

| 事件类型 | 现货持仓变化 | 合约持仓变化 | 现货资产变化 | 合约资产变化 | 份额变化 |
|----------|------------|------------|------------|------------|---------|
| 合约交易 | ❌ | ✅ 详细信息 | ❌ | -fee (USDC时) | ❌ |
| 现货交易 | ✅ coin+USDC变化 | ❌ | -fee (USDC时) | ❌ | ❌ |
| 资金费 | ❌ | ❌ | ❌ | ±usdc | ❌ |
| 充值 | ❌ | ❌ | ❌ | +usdc | ✅ |
| 提现 | ❌ | ❌ | ❌ | -(usdc+fee) | ✅ |
| 账户类别转账 | ±USDC | ❌ | ❌ | ±usdc | ❌ |
| 现货转账 | ±token | ❌ | -fee | ❌ | ✅ |
| 内部转账 | ±USDC | ❌ | -fee | ❌ | ✅ |
| 子账户转账 | ±USDC | ❌ | ❌ | ❌ | ✅ |
| 金库存款 | -USDC | ❌ | ❌ | ❌ | ✅ |
| 金库取款 | +USDC | ❌ | ❌ | ❌ | ✅ |
| 金库分配 | +USDC | ❌ | ❌ | ❌ | ✅ |
| 金库创建 | -(usdc+fee) | ❌ | ❌ | ❌ | ✅ |
| 空投 | +token | ❌ | ❌ | ❌ | ✅(可选) |
| Send | 视情况 | ❌ | 视情况 | 视情况 | 视情况 |
| 质押转账 | ❌ | ❌ | ❌ | ❌ | ❌ |
| 清算 | ❌ | ❌ | ❌ | ❌ | ❌ |
| 奖励领取 | +token/USDC | ❌ | ❌ | ❌ | ✅ |
| DEX激活 | ❌ | ❌ | ❌ | ❌ | ❌ |
| 账户激活Gas | -token | ❌ | ❌ | ❌ | ❌ |

