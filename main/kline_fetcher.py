#!/usr/bin/env python3
"""
Hyperliquid K线数据获取工具 - 简化版

代码调用示例:
    from kline_fetcher import get_open_price, get_open_prices
    
    # 查询单个时间戳的开盘价（推荐使用auto自动选择最佳精度）
    # 注意：返回的是K线开盘价，与K线时间戳对齐，避免使用未来数据
    result = get_open_price(
        coin="BTC",
        coin_type="perp",
        interval="auto",  # 自动选择最高精度（推荐）
        timestamp=1704067200000  # 时间戳必须是整数，单位毫秒
    )
    print(f"开盘价: {result['open']}")
    print(f"时间差: {result['time_diff_ms']}毫秒")
    print(f"使用周期: {result['interval']}")
    
    # 也可以手动指定时间周期
    result = get_open_price("ETH", "perp", "1h", 1704067200000)
    
    # 查询时间范围的开盘价列表
    results = get_open_prices(
        coin="ETH",
        coin_type="spot",
        interval="1h",
        start_time=1704067200000,
        end_time=1704153600000
    )
    for r in results:
        print(f"{r['time']}: {r['open']}")

    # 查询现货代币（使用@开头的编号，仅限spot类型）
    result = get_open_price("@10", "spot", "1h", 1704067200000)
    # 注意：@开头的币种只能用于spot，不能用于perp

支持的时间周期:
    - 'auto': 自动选择（根据时间距离自动选择最高精度）
    - 分钟级: 1m, 3m, 5m, 15m, 30m
    - 小时级: 1h, 2h, 4h, 8h, 12h
    - 日级: 1d, 3d

币种格式说明:
    - 永续合约 (perp): 直接使用币种名称，如 BTC, ETH
    - 现货交易对 (spot): 自动添加/USDC后缀，如 ETH -> ETH/USDC
    - 现货代币编号 (spot): @开头的编号，如 @10, @20
      * 特别注意：@开头的币种只能用于spot类型，不能用于perp类型
      * @开头的币种会保持原样查询，不会添加后缀
"""

import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

# 添加SDK路径
project_root = Path(__file__).parent.parent
sdk_path = project_root / "HyperDataCollector" / "hyperliquid-python-sdk-0.20.0"
if sdk_path.exists() and str(sdk_path) not in sys.path:
    sys.path.insert(0, str(sdk_path))

from hyperliquid.info import Info


# ============================================================================
# 工具函数
# ============================================================================

def parse_time(time_input: str) -> int:
    """将时间字符串转换为毫秒时间戳
    
    支持的格式:
    - 毫秒时间戳: 1704067200000
    - 日期: 2025-01-01
    - 日期时间: 2025-01-01 10:30:00
    
    Args:
        time_input: 时间字符串
        
    Returns:
        毫秒时间戳
    """
    # 如果已经是时间戳格式
    if time_input.isdigit():
        return int(time_input)
    
    # 尝试解析日期时间格式
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%Y/%m/%d %H:%M:%S',
        '%Y/%m/%d %H:%M',
        '%Y/%m/%d',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(time_input, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    
    raise ValueError(f"无法解析时间格式: {time_input}")


def format_timestamp(ts: int) -> str:
    """将毫秒时间戳格式化为可读字符串
    
    Args:
        ts: 毫秒时间戳
        
    Returns:
        格式化的时间字符串
    """
    return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')


def validate_interval(interval: str) -> bool:
    """验证时间周期是否有效
    
    Args:
        interval: 时间周期
        
    Returns:
        是否有效
    """
    valid_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '3d', 'auto']
    return interval in valid_intervals


def auto_select_interval(timestamp: int) -> str:
    """根据时间戳自动选择最合适的K线周期
    
    选择原则：选择能查询到数据且精度最高的周期
    Hyperliquid每个周期最多返回5000根K线
    
    Args:
        timestamp: 目标时间戳（毫秒）
        
    Returns:
        最合适的interval
    """
    from datetime import datetime
    
    # 当前时间
    current_time = int(datetime.now().timestamp() * 1000)
    
    # 时间差（毫秒）
    time_diff_ms = current_time - timestamp
    
    # 如果是未来时间，使用最小精度
    if time_diff_ms < 0:
        time_diff_ms = abs(time_diff_ms)
    
    # 各个周期的时间范围（毫秒）和对应的interval
    # 每个周期最多5000根K线，计算可查询的最大时间范围
    intervals = [
        (5000 * 60 * 1000, '1m'),           # 5000分钟 = 3.47天
        (5000 * 3 * 60 * 1000, '3m'),       # 10.4天
        (5000 * 5 * 60 * 1000, '5m'),       # 17.36天
        (5000 * 15 * 60 * 1000, '15m'),     # 52天
        (5000 * 30 * 60 * 1000, '30m'),     # 104天
        (5000 * 60 * 60 * 1000, '1h'),      # 208天
        (5000 * 2 * 60 * 60 * 1000, '2h'),  # 416天
        (5000 * 4 * 60 * 60 * 1000, '4h'),  # 833天
        (5000 * 8 * 60 * 60 * 1000, '8h'),  # 1666天
        (5000 * 12 * 60 * 60 * 1000, '12h'), # 2500天
        (5000 * 24 * 60 * 60 * 1000, '1d'),  # 13.7年
        (5000 * 3 * 24 * 60 * 60 * 1000, '3d'), # 41年
    ]
    
    # 选择第一个能覆盖时间差的周期（精度从高到低）
    for max_range, interval in intervals:
        if time_diff_ms <= max_range:
            return interval
    
    # 如果时间太久远，使用最大周期
    return '3d'


def get_available_coins() -> tuple:
    """获取所有可用的币种列表
    
    Returns:
        (所有币种, 永续合约币种, 现货币种, Info实例)
    """
    try:
        info = Info(skip_ws=True)
        
        # 获取永续合约币种
        meta = info.meta()
        perp_coins = sorted([item['name'] for item in meta.get('universe', [])])
        
        # 获取现货币种
        spot_coins = []
        spot_tokens = []
        try:
            spot_meta = info.spot_meta()
            # 获取现货tokens信息（包含全名）
            for token in spot_meta.get('tokens', []):
                name = token.get('name', '')
                full_name = token.get('fullName', '')
                spot_tokens.append({
                    'name': name,
                    'full_name': full_name,
                    'index': token.get('index', 0)
                })
            
            # 从universe中获取交易对
            for item in spot_meta.get('universe', []):
                spot_coins.append(item['name'])
            
            spot_coins = sorted(spot_coins)
        except Exception as e:
            print(f"⚠️  获取现货币种失败: {e}")
        
        # 合并去重
        all_coins = sorted(list(set(perp_coins + spot_coins)))
        
        return all_coins, perp_coins, spot_coins, spot_tokens, info
    except Exception as e:
        print(f"⚠️  无法获取币种列表: {e}")
        return [], [], [], [], None


def get_spot_token_mapping(info: Info = None) -> Dict[str, str]:
    """获取现货交易对编号到名称的映射
    
    将 @开头的编号（如 @142）映射到对应的现货名称（如 UBTC）
    
    注意：交易数据中的 @N 是 universe（交易对列表）中的 index，
    不是 tokens（代币列表）中的 index。
    
    映射逻辑：
    1. 在 universe 中找到 index=N 的交易对
    2. 获取交易对的 tokens[0]（主币，非USDC）
    3. 在 tokens 列表中找到对应的代币名称
    
    Args:
        info: Info实例（可选，如果不提供则创建新的）
        
    Returns:
        Dict[str, str]: 映射字典
        {
            '@1': 'PURR',
            '@10': 'HYPE',
            '@142': 'UBTC',
            ...
        }
        
    示例:
        mapping = get_spot_token_mapping()
        print(mapping.get('@142'))  # 输出: UBTC
        print(mapping.get('@10'))   # 输出: HYPE
    """
    try:
        if info is None:
            info = Info(skip_ws=True)
        
        spot_meta = info.spot_meta()
        tokens = spot_meta.get('tokens', [])
        universe = spot_meta.get('universe', [])
        
        # 先构建 token_index -> name 的映射
        token_index_to_name = {}
        for token in tokens:
            token_idx = token.get('index', -1)
            name = token.get('name', '')
            if name and token_idx >= 0:
                token_index_to_name[token_idx] = name
        
        # 构建 @universe_index -> name 的映射
        mapping = {}
        for pair in universe:
            universe_idx = pair.get('index', -1)
            pair_tokens = pair.get('tokens', [])
            
            if universe_idx >= 0 and len(pair_tokens) >= 1:
                # tokens[0] 是主币（非USDC），tokens[1] 通常是 USDC (index=0)
                main_token_idx = pair_tokens[0]
                name = token_index_to_name.get(main_token_idx, '')
                if name:
                    mapping[f'@{universe_idx}'] = name
        
        return mapping
        
    except Exception as e:
        print(f"⚠️  获取现货代币映射失败: {e}")
        return {}


def resolve_spot_token_id(token_id: str, info: Info = None) -> Optional[str]:
    """将 @开头的交易对编号解析为现货名称
    
    Args:
        token_id: 交易对编号（如 @142, @10）
        info: Info实例（可选）
        
    Returns:
        对应的现货名称（如 UBTC），如果找不到则返回 None
        
    示例:
        name = resolve_spot_token_id('@142')
        print(name)  # 输出: UBTC
    """
    if not token_id.startswith('@'):
        return token_id  # 如果不是 @ 开头，直接返回原值
    
    mapping = get_spot_token_mapping(info)
    return mapping.get(token_id)


def convert_coin_name(coin: str, coin_type: str, info: Info = None) -> str:
    """转换币种名称
    
    对于spot类型，如果输入的是简单币名（如ETH），自动转换为交易对（如ETH/USDC）
    特殊规则：@开头的币种（如@10、@20）只能用于spot，保持原样不转换
    
    Args:
        coin: 币种代码
        coin_type: 类型 (spot/perp)
        info: Info实例（可选）
        
    Returns:
        转换后的币种名称
        
    Raises:
        ValueError: 如果@开头的币种用于perp类型
    """
    # 检查 @ 开头的币种
    if coin.startswith('@'):
        if coin_type.lower() == 'perp':
            raise ValueError(f"错误: @ 开头的币种 '{coin}' 只能用于 spot 类型，不能用于 perp 类型")
        # spot类型，保持原样不转换
        return coin
    
    if coin_type.lower() == 'perp':
        # 永续合约直接返回
        return coin
    
    if coin_type.lower() == 'spot':
        # 如果已经包含/，直接返回
        if '/' in coin:
            return coin
        
        # 尝试添加后缀
        try:
            if info is None:
                info = Info(skip_ws=True)
            
            # Hyperliquid现货主要使用USDC，尝试顺序：/USDC, /USDT, /USD
            possible_suffixes = ['/USDC', '/USDT', '/USD']
            
            for suffix in possible_suffixes:
                test_name = coin + suffix
                if hasattr(info, 'name_to_coin'):
                    if test_name in info.name_to_coin:
                        return test_name
            
            # 如果都不存在，默认返回/USDC（Hyperliquid主要使用USDC）
            return coin + '/USDC'
            
        except Exception as e:
            # 出错时默认返回/USDC
            return coin + '/USDC'
    
    return coin


def validate_coin(coin: str, info: Info = None) -> tuple:
    """验证币种是否有效
    
    Args:
        coin: 币种代码
        info: Info实例（可选，避免重复创建）
        
    Returns:
        (是否有效, 错误信息, 建议币种列表)
    """
    try:
        if info is None:
            info = Info(skip_ws=True)
        
        # 检查是否在name_to_coin映射表中
        if hasattr(info, 'name_to_coin'):
            if coin in info.name_to_coin:
                return True, None, []
            
            # 查找相似币种（模糊匹配）
            similar_coins = []
            coin_upper = coin.upper()
            for available_coin in info.name_to_coin.keys():
                if coin_upper in available_coin.upper() or available_coin.upper() in coin_upper:
                    similar_coins.append(available_coin)
            
            # 如果没有相似币种，显示部分可用币种
            if not similar_coins:
                all_coins = list(info.name_to_coin.keys())
                similar_coins = all_coins[:10]  # 显示前10个作为示例
            
            error_msg = f"币种 '{coin}' 不存在"
            return False, error_msg, similar_coins
        else:
            # SDK版本可能不支持name_to_coin，尝试直接调用
            return True, None, []
            
    except Exception as e:
        return False, f"验证失败: {e}", []


# ============================================================================
# 核心功能
# ============================================================================

def fetch_klines(
    coin: str,
    coin_type: str,
    interval: str,
    start_time: int,
    end_time: int,
    debug: bool = False,
    skip_conversion: bool = False,
    skip_validation: bool = False,
    info: Info = None
) -> List[Dict[str, Any]]:
    """获取K线数据
    
    Args:
        coin: 币种代码
        coin_type: 类型 (spot/perp)
        interval: 时间周期 (1m/5m/15m/1h/4h/1d)
        start_time: 开始时间（毫秒时间戳）
        end_time: 结束时间（毫秒时间戳）
        debug: 是否显示调试信息（默认False）
        skip_conversion: 是否跳过币对转换（默认False）
        skip_validation: 是否跳过币种验证（默认False）
        info: Info实例（可选，如果不提供则创建新的）
        
    Returns:
        K线数据列表
        
    Raises:
        Exception: API调用失败（重试耗尽后）
    """
    # 重试配置
    max_retries = 5
    base_delay = 1.0  # 初始等待秒数
    
    if info is None:
        info = Info(skip_ws=True)
    
    # 转换币种名称（spot类型需要添加后缀）
    if not skip_conversion:
        original_coin = coin
        try:
            coin = convert_coin_name(coin, coin_type, info)
            
            if coin != original_coin and debug:
                print(f"币对转换: {original_coin} -> {coin}")
        except ValueError as e:
            # @ 开头的币种用于 perp 类型时抛出的错误
            print(f"❌ {str(e)}")
            return []
    
    # 验证币种
    if not skip_validation:
        is_valid, error_msg, similar_coins = validate_coin(coin, info)
        if not is_valid:
            print(f"❌ {error_msg}")
            if similar_coins:
                print(f"可能的币种：{', '.join(similar_coins[:5])}")
            return []
    
    # 带重试的 API 调用
    last_exception = None
    for attempt in range(max_retries):
        try:
            data = info.candles_snapshot(coin, interval, start_time, end_time)
            
            if not data:
                if debug:
                    print(f"❌ 未获取到数据 (时间周期: {interval})")
                return []
            
            return data
        
        except Exception as e:
            last_exception = e
            error_str = str(e)
            
            # 检查是否是 429 限流错误
            if '429' in error_str:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # 指数退避: 1s, 2s, 4s, 8s, 16s
                    print(f"⚠️ API 限流 (429)，等待 {delay:.1f}s 后重试 ({attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"❌ API 限流 (429)，重试 {max_retries} 次后仍失败")
            else:
                # 非 429 错误，直接抛出
                print(f"❌ 获取数据失败: {e}")
                raise
    
    # 重试耗尽，抛出最后的异常
    if last_exception:
        raise last_exception
    return []


def format_candles(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """格式化K线数据为易读格式
    
    Args:
        candles: 原始K线数据
        
    Returns:
        格式化后的K线数据
    """
    formatted = []
    for candle in candles:
        formatted.append({
            'timestamp': candle['t'],
            'time': format_timestamp(candle['t']),
            'open': candle['o'],
            'high': candle['h'],
            'low': candle['l'],
            'close': candle['c'],
            'volume': candle['v']
        })
    return formatted


def print_candles_simple(candles: List[Dict[str, Any]]):
    """简洁地打印K线数据（仅时间和收盘价）
    
    Args:
        candles: K线数据
    """
    if not candles:
        print("无数据")
        return
    
    print(f"\n共 {len(candles)} 根K线\n")
    
    # 自动检测价格精度
    max_price = max(float(candle['close']) for candle in candles)
    if max_price < 0.001:
        precision = 8
    elif max_price < 0.01:
        precision = 6
    elif max_price < 1:
        precision = 4
    else:
        precision = 2
    
    # 表头
    print(f"{'时间':<20} {'收盘价':<20}")
    print("-" * 42)
    
    # 数据行
    for candle in candles:
        close_price = float(candle['close'])
        print(f"{candle['time']:<20} {close_price:<20.{precision}f}")
    
    print(f"\n{'='*42}")
    print(f"总计: {len(candles)} 根K线")


def get_price_at_timestamp(
    coin: str,
    coin_type: str,
    interval: str,
    timestamp: int,
    debug: bool = False
) -> Optional[Dict[str, Any]]:
    """获取指定时间戳的价格（内部函数）
    
    注意：返回K线开盘价，与K线时间戳对齐。
    
    Args:
        coin: 币种代码
        coin_type: 类型 (spot/perp)
        interval: 时间周期（支持'auto'自动选择）
        timestamp: 目标时间戳（毫秒）
        debug: 是否显示调试信息（默认False）
        
    Returns:
        包含时间和价格的字典（字段名为'close'但值为开盘价），如果未找到则返回None
    """
    try:
        # 创建Info实例并进行币对转换（只转换一次）
        info = Info(skip_ws=True)
        original_coin = coin
        
        try:
            coin = convert_coin_name(coin, coin_type, info)
            
            if coin != original_coin and debug:
                print(f"币对转换: {original_coin} -> {coin}")
        except ValueError as e:
            # @ 开头的币种用于 perp 类型时抛出的错误
            print(f"❌ {str(e)}")
            return None
        
        # 验证币种（只验证一次）
        is_valid, error_msg, similar_coins = validate_coin(coin, info)
        if not is_valid:
            print(f"❌ {error_msg}")
            if similar_coins:
                print(f"可能的币种：{', '.join(similar_coins[:5])}")
            return None
        
        # 定义所有时间周期（从小到大）
        all_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '3d']
        
        # 如果是auto，自动选择最合适的interval作为起始点
        if interval == 'auto':
            interval = auto_select_interval(timestamp)
            if debug:
                print(f"自动选择时间周期: {interval}")
            
            # 从选择的interval开始，依次尝试更大的时间周期
            start_idx = all_intervals.index(interval) if interval in all_intervals else 0
            intervals_to_try = all_intervals[start_idx:]
        else:
            # 如果指定了interval，只尝试该周期
            intervals_to_try = [interval]
        
        # 根据interval计算查询范围的映射
        interval_ms = {
            '1m': 60 * 1000,
            '3m': 3 * 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '12h': 12 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
            '3d': 3 * 24 * 60 * 60 * 1000,
        }
        
        # 依次尝试不同的时间周期
        for current_interval in intervals_to_try:
            try:
                range_ms = interval_ms.get(current_interval, 60 * 60 * 1000)
                
                # 查询目标时间前后更大范围的K线（前后50个周期）
                start_time = timestamp - range_ms * 50
                end_time = timestamp + range_ms * 50
                
                # 获取K线数据（跳过币对转换和币种验证，因为已经在函数开始时处理过了）
                raw_data = fetch_klines(coin, coin_type, current_interval, start_time, end_time, 
                                       debug=debug, skip_conversion=True, skip_validation=True, info=info)
                
                if raw_data:
                    # 找到最接近目标时间的K线（不能使用未来数据）
                    # K线时间戳是开始时间，收盘价是结束时间的价格
                    # 所以要找 start_time < 目标时间 的K线（严格小于）
                    closest_candle = None
                    min_diff = float('inf')
                    
                    for candle in raw_data:
                        # 找到开始时间小于等于目标时间的K线
                        # K线的开盘价对应的是K线开始时刻的价格
                        if candle['t'] <= timestamp:
                            diff = timestamp - candle['t']  # 计算时间差（总是正数或0）
                            if diff < min_diff:
                                min_diff = diff
                                closest_candle = candle
                    
                    if closest_candle:
                        # 如果当前周期不是第一个尝试的，提示已切换周期
                        if current_interval != intervals_to_try[0] and debug:
                            print(f"⚠️  周期 {intervals_to_try[0]} 无数据，已切换到 {current_interval}")
                        
                        result = {
                            'timestamp': closest_candle['t'],
                            'time': format_timestamp(closest_candle['t']),
                            'open': float(closest_candle['o']),  # K线开盘价，与时间戳对齐
                            'time_diff_ms': closest_candle['t'] - timestamp,
                            'interval': current_interval  # 返回实际使用的interval
                        }
                        
                        # 输出调试信息（一行显示）
                        if debug:
                            time_diff_sec = abs(result['time_diff_ms']) / 1000
                            query_time = format_timestamp(timestamp)
                            print(f"[{coin}] {coin_type.upper()} | Query: {query_time} | Interval: {current_interval} | Price: ${result['open']:.8f} | TimeDiff: {time_diff_sec:.0f}s")
                        
                        return result
                else:
                    # 当前周期没有数据，继续尝试下一个周期
                    if len(intervals_to_try) > 1:
                        continue
                    
            except Exception as e:
                # 当前周期查询失败，继续尝试下一个周期
                if len(intervals_to_try) > 1:
                    continue
                else:
                    raise
        
        # 所有周期都尝试完了，仍然没有数据
        tried_intervals = ', '.join(intervals_to_try)
        print(f"❌ 所有时间周期都无法获取到数据 (已尝试: {tried_intervals})")
        return None
        
    except Exception as e:
        print(f"❌ 获取价格失败: {e}")
        return None


def save_to_json(
    candles: List[Dict[str, Any]],
    coin: str,
    interval: str,
    start_time: int,
    end_time: int,
    output_path: Optional[str] = None
) -> str:
    """保存K线数据到JSON文件
    
    Args:
        candles: K线数据
        coin: 币种
        interval: 周期
        start_time: 开始时间
        end_time: 结束时间
        output_path: 输出文件路径（可选）
        
    Returns:
        保存的文件路径
    """
    if output_path:
        filename = output_path
    else:
        # 自动生成文件名
        start_str = datetime.fromtimestamp(start_time / 1000).strftime('%Y%m%d')
        end_str = datetime.fromtimestamp(end_time / 1000).strftime('%Y%m%d')
        filename = f"{coin}_{interval}_{start_str}_{end_str}.json"
    
    # 构造输出数据
    output_data = {
        'symbol': coin,
        'interval': interval,
        'start_time': start_time,
        'end_time': end_time,
        'start_time_str': format_timestamp(start_time),
        'end_time_str': format_timestamp(end_time),
        'count': len(candles),
        'collected_at': int(datetime.now().timestamp() * 1000),
        'data': candles
    }
    
    # 保存文件
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    return filename


def save_to_csv(
    candles: List[Dict[str, Any]],
    coin: str,
    interval: str,
    start_time: int,
    end_time: int,
    output_path: Optional[str] = None
) -> str:
    """保存K线数据到CSV文件
    
    Args:
        candles: K线数据
        coin: 币种
        interval: 周期
        start_time: 开始时间
        end_time: 结束时间
        output_path: 输出文件路径（可选）
        
    Returns:
        保存的文件路径
    """
    if output_path:
        filename = output_path
    else:
        # 自动生成文件名
        start_str = datetime.fromtimestamp(start_time / 1000).strftime('%Y%m%d')
        end_str = datetime.fromtimestamp(end_time / 1000).strftime('%Y%m%d')
        filename = f"{coin}_{interval}_{start_str}_{end_str}.csv"
    
    # 写入CSV
    with open(filename, 'w', encoding='utf-8') as f:
        # 表头
        f.write('timestamp,time,open,high,low,close,volume\n')
        
        # 数据行
        for candle in candles:
            f.write(f"{candle['timestamp']},"
                   f"{candle['time']},"
                   f"{candle['open']},"
                   f"{candle['high']},"
                   f"{candle['low']},"
                   f"{candle['close']},"
                   f"{candle['volume']}\n")
    
    return filename


# ============================================================================
# 公开API接口（供代码调用）
# ============================================================================

def get_open_price(
    coin: str,
    coin_type: str,
    interval: str,
    timestamp: int,
    debug: bool = False
) -> Optional[Dict[str, Any]]:
    """获取指定时间戳的开盘价（公开API）
    
    注意：返回的是K线的开盘价，与K线时间戳对齐，避免使用未来数据。
    
    Args:
        coin: 币种代码 (如: BTC, ETH, HYPY)
        coin_type: 类型 ('perp' 或 'spot')
        interval: 时间周期，支持：
                 - 'auto': 自动选择最高精度（默认推荐）
                 - '1m', '3m', '5m', '15m', '30m': 分钟级
                 - '1h', '2h', '4h', '8h', '12h': 小时级
                 - '1d', '3d': 日级
        timestamp: 时间戳（毫秒，必须是整数）
        debug: 是否显示调试信息（默认False）
        
    Returns:
        包含以下字段的字典:
        {
            'timestamp': int,           # K线时间戳（毫秒）= K线开始时间
            'time': str,                # K线时间（可读格式）
            'open': float,              # K线开盘价
            'time_diff_ms': int,       # 与查询时间的差值（毫秒）
            'interval': str             # 实际使用的时间周期
        }
        如果查询失败则返回None
        
    示例:
        # 自动选择最佳精度
        result = get_open_price("BTC", "perp", "auto", 1704067200000)
        if result:
            print(f"开盘价: {result['open']}")
            print(f"时间差: {result['time_diff_ms']}毫秒")
            print(f"使用周期: {result['interval']}")
        
        # 指定时间周期
        result = get_open_price("BTC", "perp", "1h", 1704067200000)
        
        # 启用调试信息
        result = get_open_price("BTC", "perp", "auto", 1704067200000, debug=True)
    """
    # 验证参数类型
    if not isinstance(timestamp, int):
        print(f"错误: timestamp必须是整数类型的毫秒时间戳，当前类型: {type(timestamp).__name__}")
        return None
    
    if coin_type.lower() not in ['perp', 'spot']:
        print(f"错误: 无效的类型 '{coin_type}'，支持的类型: perp, spot")
        return None
    
    if not validate_interval(interval):
        print(f"错误: 无效的时间周期 '{interval}'")
        print(f"支持的周期: auto(推荐), 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d, 3d")
        return None
    
    # 查询价格
    return get_price_at_timestamp(coin, coin_type.lower(), interval, timestamp, debug)


def get_open_prices(
    coin: str,
    coin_type: str,
    interval: str,
    start_time: int,
    end_time: int
) -> List[Dict[str, Any]]:
    """获取时间范围内的开盘价列表（公开API）
    
    注意：返回的是K线的开盘价，与K线时间戳对齐，避免使用未来数据。
    
    Args:
        coin: 币种代码 (如: BTC, ETH, HYPY)
        coin_type: 类型 ('perp' 或 'spot')
        interval: 时间周期 ('1m'/'5m'/'15m'/'1h'/'4h'/'1d')
        start_time: 开始时间（毫秒时间戳，必须是整数）
        end_time: 结束时间（毫秒时间戳，必须是整数）
        
    Returns:
        包含时间和开盘价的字典列表:
        [
            {
                'timestamp': int,       # 时间戳（毫秒）= K线开始时间
                'time': str,            # 时间（可读格式）
                'open': float           # K线开盘价
            },
            ...
        ]
        如果查询失败则返回空列表
        
    示例:
        results = get_open_prices("ETH", "spot", "1h", 1704067200000, 1704153600000)
        for r in results:
            print(f"{r['time']}: {r['open']}")
    """
    # 验证参数类型
    if not isinstance(start_time, int):
        print(f"错误: start_time必须是整数类型的毫秒时间戳，当前类型: {type(start_time).__name__}")
        return []
    
    if not isinstance(end_time, int):
        print(f"错误: end_time必须是整数类型的毫秒时间戳，当前类型: {type(end_time).__name__}")
        return []
    
    if coin_type.lower() not in ['perp', 'spot']:
        print(f"错误: 无效的类型 '{coin_type}'，支持的类型: perp, spot")
        return []
    
    if not validate_interval(interval):
        print(f"错误: 无效的时间周期 '{interval}'，支持的周期: 1m, 5m, 15m, 1h, 4h, 1d")
        return []
    
    # 验证时间范围
    if start_time >= end_time:
        print("错误: 开始时间必须早于结束时间")
        return []
    
    # 获取K线数据
    raw_data = fetch_klines(coin, coin_type.lower(), interval, start_time, end_time)
    
    if not raw_data:
        return []
    
    # 格式化为简洁格式
    results = []
    for candle in raw_data:
        results.append({
            'timestamp': candle['t'],
            'time': format_timestamp(candle['t']),
            'open': float(candle['o'])  # K线开盘价，与时间戳对齐
        })
    
    return results


# ============================================================================
# 命令行入口
# ============================================================================

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Hyperliquid K线数据获取工具 - 简化版',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 永续合约 - 时间范围查询
  python kline_fetcher.py BTC perp 1h 1704067200000 1704153600000
  python kline_fetcher.py ETH perp 1h "2025-01-01" "2025-01-02"
  python kline_fetcher.py BTC perp 4h --days 7
  
  # 现货交易 - 时间范围查询（会自动转换币对名称）
  python kline_fetcher.py ETH spot 1h "2025-01-01" "2025-01-02"
  python kline_fetcher.py HYPY spot 4h --days 30
  
  # 现货代币编号查询（仅限spot类型）
  python kline_fetcher.py @10 spot 1h "2025-01-01" "2025-01-02"
  python kline_fetcher.py @20 spot 1h --days 7
  ⚠️  注意: @开头的币种只能用于spot类型，不能用于perp
  
  # 单个时间戳查询（查询指定时刻的收盘价）
  python kline_fetcher.py BTC perp 1h --timestamp 1704067200000
  python kline_fetcher.py ETH spot 1h --timestamp "2025-01-01 12:00:00"
  python kline_fetcher.py BTC perp 4h --timestamp "2025-01-01"
  python kline_fetcher.py @10 spot 1h --timestamp "2025-01-01"
  
  # 保存到文件
  python kline_fetcher.py BTC perp 1h "2025-01-01" "2025-01-02" -o btc_data.json
  python kline_fetcher.py BTC perp 1h "2025-01-01" "2025-01-02" --csv

支持的时间周期:
  1m, 5m, 15m, 1h, 4h, 1d
  
支持的类型:
  perp - 永续合约 (如: BTC, ETH)
  spot - 现货交易 (如: ETH, HYPY, @10, @20)
         * 普通币名会自动添加/USDC后缀
         * @开头的编号保持原样，不添加后缀
         * @开头的币种只能用于spot，不能用于perp
        """
    )
    
    parser.add_argument('coin', type=str, nargs='?', help='币种代码 (如: BTC, ETH, HYPY)')
    parser.add_argument('type', type=str, nargs='?', help='类型 (perp/spot)')
    parser.add_argument('interval', type=str, nargs='?', help='时间周期 (1m/5m/15m/1h/4h/1d)')
    parser.add_argument('start_time', nargs='?', help='开始时间 (毫秒时间戳 或 日期字符串)')
    parser.add_argument('end_time', nargs='?', help='结束时间 (毫秒时间戳 或 日期字符串)')
    
    parser.add_argument('-o', '--output', type=str, help='输出文件路径 (JSON格式)')
    parser.add_argument('--csv', action='store_true', help='导出为CSV格式')
    parser.add_argument('--days', type=int, help='获取最近N天的数据（替代start_time和end_time）')
    parser.add_argument('--timestamp', type=str, help='查询指定时间戳的收盘价（毫秒时间戳 或 日期字符串）')
    parser.add_argument('--list-coins', action='store_true', help='列出所有可用币种')
    
    args = parser.parse_args()
    
    try:
        # 如果是查看币种列表
        if args.list_coins:
            print("\n正在获取所有可用币种...")
            all_coins, perp_coins, spot_coins, spot_tokens, _ = get_available_coins()
            
            if not all_coins:
                print("❌ 无法获取币种列表")
                sys.exit(1)
            
            print(f"\n{'='*80}")
            print(f"📊 Hyperliquid 可用币种列表")
            print(f"{'='*80}\n")
            
            # 1. 永续合约
            print(f"🔷 永续合约 (Perpetuals) - 共 {len(perp_coins)} 个\n")
            print("=" * 80)
            for i in range(0, len(perp_coins), 10):
                row = perp_coins[i:i+10]
                print("  " + ", ".join(f"{coin:<8}" for coin in row))
            
            print(f"\n{'='*80}\n")
            
            # 2. 现货交易对（可读名称）
            readable_spot = [c for c in spot_coins if '/' in c or not c.startswith('@')]
            if readable_spot:
                print(f"🔶 现货交易对 (Spot Pairs) - 共 {len(readable_spot)} 个\n")
                print("=" * 80)
                for i in range(0, len(readable_spot), 6):
                    row = readable_spot[i:i+6]
                    print("  " + ", ".join(f"{coin:<12}" for coin in row))
                print(f"\n{'='*80}\n")
            
            # 3. 现货代币编号（@开头）
            indexed_spot = [c for c in spot_coins if c.startswith('@')]
            if indexed_spot:
                print(f"🔸 现货代币编号 (Spot Token IDs) - 共 {len(indexed_spot)} 个\n")
                print("=" * 80)
                print("💡 提示: @开头的是现货代币的内部编号，建议查看下方的代币映射表")
                print()
                
                # 分组显示，每行10个
                for i in range(0, min(50, len(indexed_spot)), 10):
                    row = indexed_spot[i:i+10]
                    print("  " + ", ".join(f"{coin:<8}" for coin in row))
                
                if len(indexed_spot) > 50:
                    print(f"  ... (还有 {len(indexed_spot) - 50} 个，已省略)")
                
                print(f"\n{'='*80}\n")
            
            # 4. 代币映射表（完整显示，多列布局）
            if spot_tokens:
                # 只显示有可读名称的token（不是@开头的）
                readable_tokens = [t for t in spot_tokens if not t['name'].startswith('@')]
                
                print(f"📋 现货代币映射表 (共 {len(readable_tokens)} 个)\n")
                print("=" * 100)
                print("💡 使用建议: 大部分现货代币流动性较差，推荐交易永续合约")
                print("=" * 100)
                print()
                
                # 多列显示，每行3个代币
                cols = 3
                col_width = 32  # 每列宽度
                
                for i in range(0, len(readable_tokens), cols):
                    row_tokens = readable_tokens[i:i+cols]
                    
                    # 构建每列的内容
                    row_parts = []
                    for token in row_tokens:
                        name = token['name']
                        full_name = token['full_name'] or ''
                        index = token['index']
                        
                        # 截断过长的名称
                        if len(full_name) > 18:
                            full_name = full_name[:15] + "..."
                        
                        # 格式化为 "@编号 名称 (完整名称)"
                        if full_name:
                            token_str = f"@{index:<3} {name:<8} {full_name}"
                        else:
                            token_str = f"@{index:<3} {name:<8}"
                        
                        # 填充到固定宽度
                        row_parts.append(token_str[:col_width].ljust(col_width))
                    
                    # 打印这一行
                    print("  " + " | ".join(row_parts))
                
                print(f"\n{'='*100}\n")
            
            # 5. 统计摘要
            print("📈 统计摘要:\n")
            print(f"  • 永续合约币种: {len(perp_coins)}")
            print(f"  • 现货交易对: {len(readable_spot)}")
            print(f"  • 现货代币编号: {len(indexed_spot)}")
            print(f"  • 总计: {len(all_coins)}")
            
            print(f"\n{'='*80}")
            print(f"\n💡 使用示例:")
            print(f"  # 永续合约")
            print(f"  python {sys.argv[0]} BTC 1h --days 7")
            print(f"  python {sys.argv[0]} ETH 1h --days 30")
            print(f"  ")
            print(f"  # 现货交易对（需要带/USDC后缀）")
            if readable_spot:
                example_spot = readable_spot[0]
                print(f"  python {sys.argv[0]} \"{example_spot}\" 1h --days 7")
            print(f"\n{'='*80}\n")
            
            sys.exit(0)
        
        # 正常的K线获取流程
        if not args.coin or not getattr(args, 'type', None) or not args.interval:
            print("❌ 缺少必需参数: coin, type 和 interval")
            print(f"使用 'python {sys.argv[0]} --help' 查看帮助")
            sys.exit(1)
        
        # 验证类型
        coin_type = getattr(args, 'type', '').lower()
        if coin_type not in ['perp', 'spot']:
            print(f"❌ 无效的类型: {getattr(args, 'type', '')}")
            print(f"支持的类型: perp, spot")
            sys.exit(1)
        
        # 验证时间周期
        if not validate_interval(args.interval):
            print(f"❌ 无效的时间周期: {args.interval}")
            print(f"支持的周期: 1m, 5m, 15m, 1h, 4h, 1d")
            sys.exit(1)
        
        # 如果使用 --timestamp 参数（单点查询）
        if args.timestamp:
            timestamp = parse_time(args.timestamp)
            
            print(f"\n查询参数:")
            print(f"币种: {args.coin}")
            print(f"类型: {coin_type}")
            print(f"周期: {args.interval}")
            print(f"时间戳: {format_timestamp(timestamp)}\n")
            
            # 获取指定时间戳的价格
            result = get_price_at_timestamp(args.coin, coin_type, args.interval, timestamp)
            
            if result:
                # 自动检测价格精度
                close_price = result['close']
                if close_price < 0.001:
                    precision = 8
                elif close_price < 0.01:
                    precision = 6
                elif close_price < 1:
                    precision = 4
                else:
                    precision = 2
                
                # 计算时间差
                time_diff_sec = abs(result['time_diff_ms']) / 1000
                time_diff_sign = "+" if result['time_diff_ms'] > 0 else "-" if result['time_diff_ms'] < 0 else ""
                
                print("\n" + "="*60)
                print(f"查询时间: {format_timestamp(timestamp)}")
                print(f"实际K线时间: {result['time']}")
                print(f"时间差: {time_diff_sign}{time_diff_sec:.0f}秒")
                print(f"收盘价: {close_price:.{precision}f}")
                print("="*60)
                print("\n完成！")
            else:
                print("❌ 未找到该时间戳的数据")
                sys.exit(1)
        
        else:
            # 范围查询
            # 处理时间参数
            if args.days:
                # 使用相对时间
                end_time = int(datetime.now().timestamp() * 1000)
                start_time = int((datetime.now() - timedelta(days=args.days)).timestamp() * 1000)
            else:
                # 使用绝对时间
                if not args.start_time or not args.end_time:
                    print("❌ 请提供 start_time 和 end_time，或使用 --days 参数，或使用 --timestamp 参数")
                    sys.exit(1)
                
                start_time = parse_time(args.start_time)
                end_time = parse_time(args.end_time)
            
            # 验证时间范围
            if start_time >= end_time:
                print("❌ 开始时间必须早于结束时间")
                sys.exit(1)
            
            print(f"\n查询参数:")
            print(f"币种: {args.coin}")
            print(f"类型: {coin_type}")
            print(f"周期: {args.interval}")
            print(f"时间: {format_timestamp(start_time)} ~ {format_timestamp(end_time)}\n")
            
            # 获取K线数据
            raw_data = fetch_klines(args.coin, coin_type, args.interval, start_time, end_time)
            
            if not raw_data:
                sys.exit(1)
            
            # 格式化数据
            formatted_data = format_candles(raw_data)
            
            # 简洁打印到控制台（仅时间和收盘价）
            print_candles_simple(formatted_data)
            
            # 保存到文件
            if args.output or args.csv:
                if args.csv:
                    filename = save_to_csv(
                        formatted_data, args.coin, args.interval,
                        start_time, end_time, args.output
                    )
                    print(f"\nCSV数据已保存到: {filename}")
                else:
                    filename = save_to_json(
                        formatted_data, args.coin, args.interval,
                        start_time, end_time, args.output
                    )
                    print(f"\nJSON数据已保存到: {filename}")
            
            print(f"\n完成！")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

