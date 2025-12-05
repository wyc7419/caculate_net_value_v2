# -*- coding: utf-8 -*-
"""
净值计算器 V2 - 基于逐笔持仓反推
====================================

新方法：
1. 使用 calculate_positions_backward.py 生成的逐笔持仓数据
2. 按指定时间区间聚合
3. 预加载所有币种的价格数据
4. 计算每个区间的净值

使用流程：
1. 加载 calculate_positions_backward 的 DataFrame
2. 确定统计区间（interval）
3. 生成时间区间 DataFrame
4. 预加载所有涉及币种的价格
5. 计算每个区间的净值
"""

import sys
import os
import time
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime, timedelta

# 添加模块路径
script_dir = os.path.dirname(os.path.abspath(__file__))

# 使用相对导入（同一包内的模块）
from .calculate_positions_backward import PositionBackwardCalculator
from .kline_fetcher import get_open_prices


class NetValueCalculatorV2:
    """净值计算器 V2 - 基于逐笔持仓反推"""
    
    # 支持的时间区间（从配置读取）
    from config.settings import SUPPORTED_INTERVALS
    
    def __init__(self, address: str, interval: str = '1h', debug: bool = False):
        """
        初始化净值计算器 V2
        
        参数:
            address: 账户地址
            interval: 时间区间，支持 '1h', '2h', '4h', '8h', '12h', '1d'
            debug: 是否显示调试信息
        """
        if interval not in self.SUPPORTED_INTERVALS:
            raise ValueError(f"不支持的时间区间: {interval}，支持的区间: {', '.join(self.SUPPORTED_INTERVALS)}")
        
        self.address = address
        self.interval = interval
        self.debug = debug
        
        self.positions_df = None  # 逐笔持仓数据
        self.intervals_df = None  # 时间区间数据（包含价格）
        self.first_trade_timestamp = None  # 第一笔交易时间戳（毫秒）
        
        # 统计API调用次数
        self.api_call_count = 0
        self.cache_hit_count = 0
    
    def load_positions_data(self) -> bool:
        """
        步骤1：加载逐笔持仓数据
        
        返回:
            bool: 是否成功加载
        """
        print("\n" + "="*80)
        print("步骤1：加载逐笔持仓数据")
        print("="*80)
        
        try:
            # 创建持仓反推计算器（从 API 获取数据）
            calculator = PositionBackwardCalculator(
                address=self.address,
                export_csv=False  # 不需要导出CSV
            )
            
            # 计算逐笔持仓（会自动从 API 加载快照和事件数据）
            self.positions_df = calculator.calculate_backward()
            
            if self.positions_df is None or len(self.positions_df) == 0:
                print("❌ 获取持仓数据失败")
                return False
            
            print(f"✅ 成功加载逐笔持仓数据")
            print(f"   事件总数: {len(self.positions_df)}")
            print(f"   时间范围: {self.positions_df.iloc[0]['time']} 至 {self.positions_df.iloc[-1]['time']}")
            
            # 查找第一笔交易（perps 或 spot 类型）
            self._find_first_trade_timestamp()
            
            return True
            
        except Exception as e:
            print(f"❌ 加载持仓数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _find_first_trade_timestamp(self):
        """
        查找第一笔交易的时间戳
        
        交易类型：event_category == 'trade' 且 event_type in ['perps', 'spot']
        """
        if self.positions_df is None or len(self.positions_df) == 0:
            return
        
        # 筛选交易事件（perps 或 spot）
        # positions_df 是按时间正序排列的（最早在前）
        trade_events = self.positions_df[
            (self.positions_df['event_category'] == 'trade') & 
            (self.positions_df['event_type'].isin(['perps', 'spot', 'perp']))
        ]
        
        if len(trade_events) > 0:
            # 取第一行（时间最早的交易）
            self.first_trade_timestamp = int(trade_events.iloc[0]['timestamp'])
            first_trade_time = trade_events.iloc[0]['time']
            first_trade_type = trade_events.iloc[0]['event_type']
            print(f"   第一笔交易: {first_trade_time} (类型: {first_trade_type})")
        else:
            print("   ⚠️  未找到交易事件")
    
    def get_first_trade_timestamp(self) -> int:
        """获取第一笔交易时间戳（毫秒）"""
        return self.first_trade_timestamp
    
    def _parse_interval_to_seconds(self, interval: str) -> int:
        """将时间区间字符串转换为秒数"""
        unit = interval[-1]
        value = int(interval[:-1])
        
        if unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        else:
            raise ValueError(f"无法解析的时间区间: {interval}")
    
    def generate_time_intervals(self) -> bool:
        """
        步骤2：生成时间区间 DataFrame
        
        时间范围：
        - 起始时间：最早事件的时间戳，向下取整到区间边界
        - 结束时间：最后一个 is_snapshot_recorded=True 的事件时间，向下取整到区间边界
        
        注意：positions_df 是按时间正序排列的（最早在前，最新在后）
        
        返回:
            bool: 是否成功生成
        """
        print("\n" + "="*80)
        print("步骤2：生成时间区间")
        print("="*80)
        
        if self.positions_df is None:
            print("❌ 尚未加载持仓数据")
            return False
        
        # positions_df 是按时间正序排列的（最早在前，最新在后）
        # 起始时间：最早事件（DataFrame 的第一行）
        start_timestamp = int(self.positions_df.iloc[0]['timestamp'])
        
        # 结束时间：最后一个 is_snapshot_recorded=True 的事件
        # 由于是正序，我们需要找索引最大的那个 True（时间上最新的快照）
        if 'is_snapshot_recorded' in self.positions_df.columns:
            snapshot_rows = self.positions_df[self.positions_df['is_snapshot_recorded'] == True]
            if not snapshot_rows.empty:
                # 取最后一行（时间最新的快照事件）
                end_timestamp = int(snapshot_rows.iloc[-1]['timestamp'])
                print(f"   找到 {len(snapshot_rows)} 个快照记录")
                print(f"   最新快照时间: {datetime.fromtimestamp(end_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                # 没有快照记录，使用最新事件时间
                print("⚠️  未找到 is_snapshot_recorded=True 的记录，使用最新事件时间")
                end_timestamp = int(self.positions_df.iloc[-1]['timestamp'])
        else:
            # 没有 is_snapshot_recorded 列，使用最新事件时间
            print("⚠️  positions_df 中没有 is_snapshot_recorded 列，使用最新事件时间")
        end_timestamp = int(self.positions_df.iloc[-1]['timestamp'])
        
        # 转换为秒并向下取整到区间边界
        interval_seconds = self._parse_interval_to_seconds(self.interval)
        interval_ms = interval_seconds * 1000
        
        # 对齐到区间边界（都向下取整）
        start_aligned = (start_timestamp // interval_ms) * interval_ms
        end_aligned = ((end_timestamp // interval_ms) + 1) * interval_ms  # +1 确保包含结束区间
        
        # 生成时间戳序列
        timestamps = list(range(start_aligned, end_aligned, interval_ms))
        
        # 创建 DataFrame
        self.intervals_df = pd.DataFrame({
            'timestamp': timestamps
        })
        
        # 添加时间字符串列
        self.intervals_df['time'] = self.intervals_df['timestamp'].apply(
            lambda ts: datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
        )
        
        print(f"✅ 生成时间区间成功")
        print(f"   区间: {self.interval}")
        print(f"   起始时间: {self.intervals_df.iloc[0]['time']} (时间戳: {self.intervals_df.iloc[0]['timestamp']})")
        print(f"   结束时间: {self.intervals_df.iloc[-1]['time']} (时间戳: {self.intervals_df.iloc[-1]['timestamp']})")
        print(f"   区间数量: {len(self.intervals_df)}")
        
        return True
    
    def _extract_coins_from_positions(self, positions_str: str, is_perp: bool = False) -> set:
        """从持仓字符串中提取币种列表"""
        import json
        
        if not positions_str or positions_str == '':
            return set()
        
        coins = set()
        
        try:
            # 规范化字符串
            positions_str_normalized = positions_str.replace("'", '"')
            positions_data = json.loads(positions_str_normalized)
            
            if isinstance(positions_data, list):
                # 列表格式（合约双向持仓）
                for item in positions_data:
                    if 'coin' in item:
                        coins.add(item['coin'])
            elif isinstance(positions_data, dict):
                # 字典格式
                coins.update(positions_data.keys())
        
        except Exception as e:
            if self.debug:
                print(f"⚠️  警告: 解析持仓失败: {positions_str[:50]}... 错误: {e}")
        
        return coins
    
    def extract_all_coins(self) -> tuple:
        """
        提取所有涉及的币种
        
        返回:
            (spot_coins, perp_coins): 现货币种集合，合约币种集合
        """
        print("\n" + "="*80)
        print("步骤3：提取所有涉及的币种")
        print("="*80)
        
        spot_coins = set()
        perp_coins = set()
        
        # 遍历所有持仓记录
        for idx, row in self.positions_df.iterrows():
            # 提取现货币种
            spot_positions = row.get('spot_positions', '')
            spot_coins.update(self._extract_coins_from_positions(spot_positions, is_perp=False))
            
            # 提取合约币种
            perp_positions = row.get('perp_positions', '')
            perp_coins.update(self._extract_coins_from_positions(perp_positions, is_perp=True))
        
        # 移除 USDC（价格固定为1）
        spot_coins.discard('USDC')
        
        print(f"✅ 提取币种完成")
        print(f"   现货币种数: {len(spot_coins)}")
        print(f"   现货币种: {sorted(spot_coins)}")
        print(f"   合约币种数: {len(perp_coins)}")
        print(f"   合约币种: {sorted(perp_coins)}")
        
        return spot_coins, perp_coins
    
    def preload_prices(self, spot_coins: set, perp_coins: set) -> bool:
        """
        步骤4：预加载所有币种的价格数据
        
        参数:
            spot_coins: 现货币种集合
            perp_coins: 合约币种集合
        
        返回:
            bool: 是否成功加载
        """
        print("\n" + "="*80)
        print("步骤4：预加载价格数据")
        print("="*80)
        
        if self.intervals_df is None:
            print("❌ 尚未生成时间区间")
            return False
        
        # 获取时间范围
        start_time = int(self.intervals_df.iloc[0]['timestamp'])
        end_time = int(self.intervals_df.iloc[-1]['timestamp'])
        
        print(f"\n时间范围: {start_time} - {end_time}")
        print(f"需要加载 {len(spot_coins)} 个现货币种和 {len(perp_coins)} 个合约币种的价格")
        
        # 定义时间戳转换函数（用于匹配价格数据）
        def ts_to_key(ts_ms):
            """将时间戳转换为匹配键"""
            if self.interval in ['1d', '3d']:
                return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d')
            elif self.interval in ['1h', '2h', '4h', '8h', '12h']:
                return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H')
            else:
                return ts_ms
        
        # 加载现货币种价格
        spot_coins_sorted = sorted(spot_coins)
        for i, coin in enumerate(spot_coins_sorted):
            print(f"   正在加载 {coin} (spot) 价格...")
            try:
                prices_data = get_open_prices(
                    coin=coin,
                    coin_type='spot',
                    interval=self.interval,
                    start_time=start_time,
                    end_time=end_time
                )
                
                if prices_data:
                    # 创建价格字典
                    price_dict = {}
                    for item in prices_data:
                        ts = item.get('timestamp', 0)
                        price = item.get('open', 0)
                        if ts:
                            key = ts_to_key(ts)
                            price_dict[key] = price
                    
                    # 添加价格列
                    column_name = f'{coin}_spot_price'
                    self.intervals_df[column_name] = self.intervals_df['timestamp'].map(
                        lambda ts: price_dict.get(ts_to_key(ts), 0)
                    )
                    print(f"      ✓ 已添加 {column_name} 列")
                else:
                    print(f"      ✗ 未获取到数据")
            except Exception as e:
                print(f"      ✗ 获取失败: {e}")
            
            # 请求间隔，避免触发限流（最后一个不需要等待）
            if i < len(spot_coins_sorted) - 1:
                time.sleep(0.15)
        
        # 加载合约币种价格
        perp_coins_sorted = sorted(perp_coins)
        for i, coin in enumerate(perp_coins_sorted):
            print(f"   正在加载 {coin} (perp) 价格...")
            try:
                prices_data = get_open_prices(
                    coin=coin,
                    coin_type='perp',
                    interval=self.interval,
                    start_time=start_time,
                    end_time=end_time
                )
                
                if prices_data:
                    # 创建价格字典
                    price_dict = {}
                    for item in prices_data:
                        ts = item.get('timestamp', 0)
                        price = item.get('open', 0)
                        if ts:
                            key = ts_to_key(ts)
                            price_dict[key] = price
                    
                    # 添加价格列
                    column_name = f'{coin}_perp_price'
                    self.intervals_df[column_name] = self.intervals_df['timestamp'].map(
                        lambda ts: price_dict.get(ts_to_key(ts), 0)
                    )
                    print(f"      ✓ 已添加 {column_name} 列")
                else:
                    print(f"      ✗ 未获取到数据")
            except Exception as e:
                print(f"      ✗ 获取失败: {e}")
            
            # 请求间隔，避免触发限流（最后一个不需要等待）
            if i < len(perp_coins_sorted) - 1:
                time.sleep(0.15)
        
        print(f"\n✅ 价格数据加载完成")
        print(f"   总列数: {len(self.intervals_df.columns)}")
        print(f"   价格列数: {len(self.intervals_df.columns) - 2}")  # 减去 timestamp 和 time 列
        
        return True
    
    def initialize(self) -> bool:
        """
        初始化：完成前4步
        
        返回:
            bool: 是否成功初始化
        """
        print("="*80)
        print("净值计算器 V2 - 初始化")
        print("="*80)
        print(f"\n账户地址: {self.address}")
        print(f"时间区间: {self.interval}")
        
        # 步骤1：加载逐笔持仓数据
        if not self.load_positions_data():
            return False
        
        # 步骤2：生成时间区间
        if not self.generate_time_intervals():
            return False
        
        # 步骤3：提取所有币种
        spot_coins, perp_coins = self.extract_all_coins()
        
        # 步骤4：预加载价格
        if not self.preload_prices(spot_coins, perp_coins):
            return False
        
        print("\n" + "="*80)
        print("✅ 初始化完成！")
        print("="*80)
        print(f"\n数据统计:")
        print(f"   逐笔事件数: {len(self.positions_df)}")
        print(f"   时间区间数: {len(self.intervals_df)}")
        print(f"   涉及币种数: {len(spot_coins) + len(perp_coins)}")
        print(f"   DataFrame 列数: {len(self.intervals_df.columns)}")
        
        return True

# ==================== 计算现货账户价值 ====================
# ==================== 计算现货账户价值 ====================

    def _parse_spot_positions(self, positions_str: str) -> Dict[str, float]:
        """
        解析现货持仓字符串为字典
        
        参数:
            positions_str: 持仓字符串，如 "{'BTC': 10.5, 'USDC': 50000}"
        
        返回:
            Dict[str, float]: 币种->数量的字典
        """
        import json
        
        if not positions_str or positions_str == '':
            return {}
        
        try:
            # 规范化字符串
            positions_str_normalized = positions_str.replace("'", '"')
            positions_dict = json.loads(positions_str_normalized)
            
            # 转换为简单的 {币种: 数量} 字典
            result = {}
            for coin, value in positions_dict.items():
                if isinstance(value, dict):
                    # 如果是字典格式 {'amount': 10.5}
                    result[coin] = float(value.get('amount', 0))
                else:
                    # 如果直接是数值
                    result[coin] = float(value)
            
            return result
        
        except Exception as e:
            if self.debug:
                print(f"⚠️  警告: 解析现货持仓失败: {positions_str[:50]}... 错误: {e}")
            return {}
    
    def _find_position_before(self, target_timestamp: int, position_type: str = 'spot') -> str:
        """
        找到目标时间戳之前最近的持仓记录
        
        重要：只能使用目标时间戳**之前**的持仓记录，不能使用之后的
        例如：16:00:00 只能使用 <= 16:00:00 的持仓，不能使用 16:26:01 的持仓
        
        注意：如果有多个相同时间戳的记录，选取最后一个（索引最大的），
        因为它代表该时间点所有事件处理完毕后的持仓状态。
        
        参数:
            target_timestamp: 目标时间戳（毫秒）
            position_type: 持仓类型，'spot' 或 'perp'
        
        返回:
            str: 持仓字符串，如果该时间之前没有任何持仓记录，返回空字符串
        """
        if self.positions_df is None or len(self.positions_df) == 0:
            return ''
        
        # 只选择时间戳 <= target_timestamp 的记录
        before_df = self.positions_df[self.positions_df['timestamp'] <= target_timestamp]
        
        if len(before_df) == 0:
            # 该时间点之前没有任何持仓记录，返回空持仓
            return ''
        
        # 找到时间戳最大的值
        max_timestamp = before_df['timestamp'].max()
        
        # 在所有最大时间戳的记录中，选择索引最大的（即最后一条）
        # 这样可以确保选取的是该时间点所有事件处理完毕后的持仓
        same_ts_df = before_df[before_df['timestamp'] == max_timestamp]
        nearest_idx = same_ts_df.index[-1]
        
        # 返回对应的持仓
        column = 'spot_positions' if position_type == 'spot' else 'perp_positions'
        return self.positions_df.loc[nearest_idx, column]
    
    def calculate_spot_account_value(self) -> bool:
        """
        步骤5：计算每个区间的现货账户价值
        
        逻辑：
        1. 对于每个区间，找到该时间点**之前**最近的 spot_positions
           - 如果该时间点之前没有任何交易，则持仓为空（价值为0）
           - 例如：16:00:00 的区间，只能使用 <= 16:00:00 的持仓
        2. 解析持仓，获取每个币种的数量
        3. 使用预加载的价格计算价值
        4. 累加得到 spot_account_value
        
        返回:
            bool: 是否成功计算
        """
        print("\n" + "="*80, flush=True)
        print("步骤5：计算现货账户价值", flush=True)
        print("="*80, flush=True)
        
        if self.intervals_df is None:
            print("❌ 尚未生成时间区间", flush=True)
            return False
        
        # 添加现货账户价值列
        self.intervals_df['spot_account_value'] = 0.0
        self.intervals_df['spot_positions'] = ''
        
        total_intervals = len(self.intervals_df)
        print(f"开始处理 {total_intervals} 个区间...\n", flush=True)
        
        for idx, row in self.intervals_df.iterrows():
            interval_timestamp = int(row['timestamp'])
            
            # 1. 找到该时间点之前最近的 spot_positions
            spot_positions_str = self._find_position_before(interval_timestamp, 'spot')
            
            # 保存到 DataFrame
            self.intervals_df.at[idx, 'spot_positions'] = spot_positions_str
            
            # 2. 解析持仓
            spot_positions = self._parse_spot_positions(spot_positions_str)
            
            if not spot_positions:
                self.intervals_df.at[idx, 'spot_account_value'] = 0.0
                continue
            
            # 3. 计算账户价值
            account_value = 0.0
            
            for coin, amount in spot_positions.items():
                if abs(amount) < 1e-10:
                    continue
                
                # USDC 价格固定为 1.0
                if coin == 'USDC':
                    price = 1.0
                else:
                    # 从预加载的价格列获取价格
                    price_column = f'{coin}_spot_price'
                    if price_column in self.intervals_df.columns:
                        price = self.intervals_df.at[idx, price_column]
                        self.cache_hit_count += 1
                    else:
                        if self.debug:
                            print(f"⚠️  警告: 未找到 {coin} 的现货价格列 (时间: {row['time']})")
                        price = 0
                
                if price and price > 0:
                    account_value += amount * price
            
            # 4. 保存到 DataFrame
            self.intervals_df.at[idx, 'spot_account_value'] = account_value
            
            # 显示进度（每1000个区间或最后一个）
            if (idx + 1) % 1000 == 0 or (idx + 1) == total_intervals:
                progress_pct = ((idx + 1) / total_intervals) * 100
                print(f"  已处理 {idx + 1}/{total_intervals} 个区间 ({progress_pct:.1f}%)...", flush=True)
        
        print(f"\n✅ 现货账户价值计算完成！", flush=True)
        print(f"   缓存命中次数: {self.cache_hit_count}", flush=True)
        
        # 显示统计信息
        if len(self.intervals_df) > 0:
            first_value = self.intervals_df.iloc[0]['spot_account_value']
            last_value = self.intervals_df.iloc[-1]['spot_account_value']
            print(f"   最早区间现货价值: ${first_value:,.2f}", flush=True)
            print(f"   最新区间现货价值: ${last_value:,.2f}", flush=True)
        
        return True


# ==================== 计算合约账户价值 ====================
# ==================== 计算合约账户价值 ====================
    def _parse_perp_positions(self, positions_str: str) -> List[Dict]:
        """
        解析合约持仓字符串为列表
        
        参数:
            positions_str: 持仓字符串，如 "[{'coin': 'BTC', 'amount': 10, 'dir': 'long'}]"
        
        返回:
            List[Dict]: 持仓列表
        """
        import json
        
        if not positions_str or positions_str == '':
            return []
        
        try:
            # 规范化字符串
            positions_str_normalized = positions_str.replace("'", '"')
            positions_data = json.loads(positions_str_normalized)
            
            if isinstance(positions_data, list):
                return positions_data
            elif isinstance(positions_data, dict):
                # 转换字典格式为列表格式
                result = []
                for coin, info in positions_data.items():
                    result.append({
                        'coin': coin,
                        'amount': info.get('amount', 0),
                        'dir': info.get('dir', 'long')
                    })
                return result
            else:
                return []
        
        except Exception as e:
            if self.debug:
                print(f"⚠️  警告: 解析合约持仓失败: {positions_str[:50]}... 错误: {e}")
            return []
    
    def _get_trades_in_interval(self, start_timestamp: int, end_timestamp: int) -> List[Dict]:
        """
        获取时间区间内的所有合约交易
        
        参数:
            start_timestamp: 起始时间戳（毫秒）
            end_timestamp: 结束时间戳（毫秒）
        
        返回:
            List[Dict]: 交易列表，每个交易包含 coin, amount, price, dir, side, time
        """
        if self.positions_df is None:
            return []
        
        # 筛选时间区间内的记录
        mask = (self.positions_df['timestamp'] > start_timestamp) & (self.positions_df['timestamp'] <= end_timestamp)
        interval_df = self.positions_df[mask]
        
        trades = []
        
        for idx, row in interval_df.iterrows():
            # 解析合约持仓变化
            perp_changes_str = row.get('perp_position_changes', '')
            if not perp_changes_str or perp_changes_str == '':
                continue
            
            # 获取原始事件的时间
            event_time = row.get('time', '')
            
            # 解析交易
            try:
                import json
                perp_changes_str_normalized = perp_changes_str.replace("'", '"')
                perp_changes_data = json.loads(perp_changes_str_normalized)
                
                # 格式：{币种名: {详细信息}}
                if isinstance(perp_changes_data, dict):
                    for coin, trade_info in perp_changes_data.items():
                        if isinstance(trade_info, dict):
                            trades.append({
                                'coin': coin,
                                'amount': float(trade_info.get('amount', 0)),
                                'price': float(trade_info.get('price', 0)),
                                'dir': trade_info.get('dir', ''),
                                'side': trade_info.get('side', ''),
                                'time': event_time  # 添加原始事件时间
                            })
                # 旧格式兼容：[{coin, amount, ...}]
                elif isinstance(perp_changes_data, list):
                    for trade_info in perp_changes_data:
                        trades.append({
                            'coin': trade_info.get('coin', ''),
                            'amount': float(trade_info.get('amount', 0)),
                            'price': float(trade_info.get('price', 0)),
                            'dir': trade_info.get('dir', ''),
                            'side': trade_info.get('side', ''),
                            'time': event_time  # 添加原始事件时间
                        })
            
            except Exception as e:
                if self.debug:
                    print(f"⚠️  警告: 解析合约交易失败: {perp_changes_str[:50]}... 错误: {e}")
        
        return trades
    
    def _get_perp_asset_changes_in_interval(self, start_timestamp: int, end_timestamp: int) -> float:
        """
        获取时间区间内的所有合约资产变化（资金费率等）
        
        参数:
            start_timestamp: 起始时间戳（毫秒）
            end_timestamp: 结束时间戳（毫秒）
        
        返回:
            float: 资产变化总和
        """
        if self.positions_df is None:
            return 0.0
        
        # 筛选时间区间内的记录
        mask = (self.positions_df['timestamp'] > start_timestamp) & (self.positions_df['timestamp'] <= end_timestamp)
        interval_df = self.positions_df[mask]
        
        total_change = 0.0
        
        for idx, row in interval_df.iterrows():
            asset_change_str = row.get('perp_asset_change_ex_position', '')
            if asset_change_str and asset_change_str != '':
                try:
                    total_change += float(asset_change_str)
                except (ValueError, TypeError):
                    pass
        
        return total_change
    
    # ==================== FIFO 交易处理方法（参考 calculate_net_value_optimized.py）====================
    
    def _process_open_long(self, queue: List, amount: float, price: float) -> float:
        """开多头"""
        queue.append((price, amount))
        return 0.0
    
    def _process_open_short(self, queue: List, amount: float, price: float) -> float:
        """开空头"""
        queue.append((price, -amount))
        return 0.0
    
    def _process_close_long(self, queue: List, amount: float, price: float, coin: str = None, time: str = None) -> float:
        """平多头"""
        realized_pnl = 0.0
        to_close = amount
        i = 0
        
        while to_close > 1e-10 and i < len(queue):
            open_price, open_amount = queue[i]
            
            if open_amount > 1e-10:  # 是多头，处理
                if open_amount <= to_close:
                    pnl = (price - open_price) * open_amount
                    realized_pnl += pnl
                    to_close -= open_amount
                    queue.pop(i)  # 删除当前元素，i不变
                else:
                    pnl = (price - open_price) * to_close
                    realized_pnl += pnl
                    queue[i] = (open_price, open_amount - to_close)
                    to_close = 0
            else:  # 不是多头（是空头），跳过
                i += 1
        
        if to_close > 1e-10 and to_close >= 0.01:
            if self.debug:
                time_info = f" (时间: {time})" if time else ""
                print(f"⚠️  警告: [{coin}] 平多数量不足！还需平 {to_close:.8f}{time_info}")
        
        return realized_pnl
    
    def _process_close_short(self, queue: List, amount: float, price: float, coin: str = None, time: str = None) -> float:
        """平空头"""
        realized_pnl = 0.0
        to_close = amount
        i = 0
        
        while to_close > 1e-10 and i < len(queue):
            open_price, open_amount = queue[i]
            
            if open_amount < -1e-10:  # 是空头，处理
                close_amount = min(abs(open_amount), to_close)
                pnl = (open_price - price) * close_amount
                realized_pnl += pnl
                
                if abs(open_amount) <= to_close + 1e-10:
                    to_close -= abs(open_amount)
                    queue.pop(i)  # 删除当前元素，i不变
                else:
                    queue[i] = (open_price, open_amount + to_close)
                    to_close = 0
            else:  # 不是空头（是多头），跳过
                i += 1
        
        if to_close > 1e-10 and to_close >= 0.01:
            if self.debug:
                time_info = f" (时间: {time})" if time else ""
                print(f"⚠️  警告: [{coin}] 平空数量不足！还需平 {to_close:.8f}{time_info}")
        
        return realized_pnl
    
    def _process_short_to_long(self, queue: List, amount: float, price: float, coin: str = None) -> float:
        """空翻多"""
        realized_pnl = 0.0
        current_short = sum(abs(amt) for _, amt in queue if amt < -1e-10)
        
        if current_short > 1e-10:
            to_close = min(current_short, amount)
            closed = 0.0
            i = 0
            
            while closed < to_close - 1e-10 and i < len(queue):
                if queue[i][1] < -1e-10:
                    open_price, open_amount = queue[i]
                    close_amount = min(abs(open_amount), to_close - closed)
                    pnl = (open_price - price) * close_amount
                    realized_pnl += pnl
                    closed += close_amount
                    
                    if abs(open_amount) <= close_amount + 1e-10:
                        queue.pop(i)
                    else:
                        queue[i] = (open_price, open_amount + close_amount)
                        i += 1
                else:
                    i += 1
        
        long_amount = amount - current_short
        if long_amount > 1e-10:
            queue.append((price, long_amount))
        
        return realized_pnl
    
    def _process_long_to_short(self, queue: List, amount: float, price: float) -> float:
        """多翻空"""
        realized_pnl = 0.0
        current_long = sum(amt for _, amt in queue if amt > 1e-10)
        
        if current_long > 1e-10:
            to_close = min(current_long, amount)
            closed = 0.0
            i = 0
            
            while closed < to_close - 1e-10 and i < len(queue):
                if queue[i][1] > 1e-10:
                    open_price, open_amount = queue[i]
                    close_amount = min(open_amount, to_close - closed)
                    pnl = (price - open_price) * close_amount
                    realized_pnl += pnl
                    closed += close_amount
                    
                    if open_amount <= close_amount + 1e-10:
                        queue.pop(i)
                    else:
                        queue[i] = (open_price, open_amount - close_amount)
                        i += 1
                else:
                    i += 1
        
        short_amount = amount - current_long
        if short_amount > 1e-10:
            queue.append((price, -short_amount))
        
        return realized_pnl
    
    def _process_auto_deleveraging(self, queue: List, amount: float, price: float, side: str = None) -> float:
        """自动减仓（ADL）"""
        if not queue:
            if self.debug:
                print(f"⚠️  警告: ADL时队列为空")
            return 0.0
        
        if side:
            if side == "B":
                return self._process_close_short(queue, amount, price)
            elif side == "A":
                return self._process_close_long(queue, amount, price)
            else:
                if self.debug:
                    print(f"⚠️  警告: ADL的side参数无效: {side}")
        
        for open_price, open_amount in queue:
            if open_amount > 1e-10:
                return self._process_close_long(queue, amount, price)
            elif open_amount < -1e-10:
                return self._process_close_short(queue, amount, price)
        
        if self.debug:
            print(f"⚠️  警告: ADL时无法判断持仓方向")
        return 0.0
    
    def _process_liquidation(self, queue: List, amount: float, price: float, dir_type: str) -> float:
        """清算"""
        if 'Long' in dir_type:
            return self._process_close_long(queue, amount, price)
        elif 'Short' in dir_type:
            return self._process_close_short(queue, amount, price)
        else:
            if self.debug:
                print(f"⚠️  警告: 无法识别清算类型: {dir_type}")
            return 0.0
    
    def _process_settlement(self, queue: List, amount: float, price: float) -> float:
        """结算"""
        realized_pnl = 0.0
        
        while queue:
            open_price, open_amount = queue.pop(0)
            
            if open_amount > 1e-10:
                pnl = (price - open_price) * open_amount
            elif open_amount < -1e-10:
                pnl = (open_price - price) * abs(open_amount)
            else:
                pnl = 0.0
            
            realized_pnl += pnl
        
        return realized_pnl

    def calculate_perp_account_value(self) -> bool:
        """
        步骤6：计算每个区间的合约账户价值（每个区间重新虚拟开仓）
        
        新逻辑：
        对于每个区间 T_n → T_n+1：
        1. 用 T_n 的价格重新虚拟开仓所有持仓（初始化队列）
        2. 应用区间内的交易，计算 realized_pnl（使用真实交易价格）
        3. 用 T_n+1 的价格虚拟平仓，计算 virtual_pnl
        4. perp_account_value = 上一行 + realized_pnl + virtual_pnl + asset_changes
        
        优点：避免虚拟开仓价不准确的累积误差
        
        返回:
            bool: 是否成功计算
        """
        print("\n" + "="*80, flush=True)
        print("步骤6：计算合约账户价值（每个区间重新虚拟开仓）", flush=True)
        print("="*80, flush=True)
        
        if self.intervals_df is None:
            print("❌ 尚未生成时间区间", flush=True)
            return False
        
        # 添加合约相关列
        self.intervals_df['perp_positions'] = ''
        self.intervals_df['perp_queue_positions'] = ''  # FIFO计算后的队列持仓
        self.intervals_df['perp_account_value'] = 0.0
        self.intervals_df['realized_pnl'] = 0.0
        self.intervals_df['virtual_pnl'] = 0.0
        
        # 初始化：第一个区间
        first_timestamp = int(self.intervals_df.iloc[0]['timestamp'])
        first_perp_positions_str = self._find_position_before(first_timestamp, 'perp')
        first_perp_positions = self._parse_perp_positions(first_perp_positions_str)
        
        # 构建第一个区间的持仓字符串（与循环中的逻辑一致）
        first_perp_positions_formatted = []
        for pos in first_perp_positions:
            first_perp_positions_formatted.append({
                'coin': pos['coin'],
                'amount': pos['amount'],
                'dir': pos.get('dir', 'long' if pos['amount'] > 0 else 'short')
            })
        first_perp_positions_str_formatted = str(first_perp_positions_formatted).replace('"', "'") if first_perp_positions_formatted else ''
        
        self.intervals_df.at[0, 'perp_positions'] = first_perp_positions_str_formatted
        self.intervals_df.at[0, 'perp_queue_positions'] = ''  # 第一个区间不记录队列
        self.intervals_df.at[0, 'perp_account_value'] = 0.0  # 初始账户价值设为0
        self.intervals_df.at[0, 'realized_pnl'] = 0.0
        self.intervals_df.at[0, 'virtual_pnl'] = 0.0

        # 从第二个区间开始正向推进
        total_intervals = len(self.intervals_df)
        print(f"开始处理 {total_intervals - 1} 个区间...\n", flush=True)
        
        # 用于记录已经警告过的持仓不匹配（避免重复警告）
        warned_mismatches = set()  # 存储 (coin, dir) 元组
        
        for idx in range(1, total_intervals):
            prev_idx = idx - 1
            
            # 当前区间的起止时间
            start_timestamp = int(self.intervals_df.iloc[prev_idx]['timestamp'])
            end_timestamp = int(self.intervals_df.iloc[idx]['timestamp'])
            
            # 获取上一个区间的账户价值
            prev_account_value = float(self.intervals_df.at[prev_idx, 'perp_account_value'])
            
            # ========== 🔍 关键修改：每个区间重新初始化队列 ==========
            # 步骤1：获取区间开始时（上一区间结束时）的持仓
            # 🔧 修复：使用实际持仓而非FIFO计算出的持仓，避免错误传播
            actual_positions_str = self._find_position_before(start_timestamp, 'perp')
            prev_perp_positions = self._parse_perp_positions(actual_positions_str)
            
            # 步骤2：用区间开始时的价格重新虚拟开仓，初始化队列
            position_queues = {}  # 🔍 每个区间都重新创建队列
            
            for position in prev_perp_positions:
                coin = position['coin']
                amount = position['amount']
                
                # 获取区间开始时的价格（= 上一区间结束时的价格）
                price_col = f'{coin}_perp_price'
                if price_col in self.intervals_df.columns:
                    start_price = self.intervals_df.iloc[prev_idx][price_col]
                    if start_price and start_price > 0:
                        if coin not in position_queues:
                            position_queues[coin] = []
                        
                        # 用区间开始价格虚拟开仓
                        if amount > 0:  # 多头
                            position_queues[coin].append((start_price, amount))
                        else:  # 空头
                            position_queues[coin].append((start_price, amount))
            
            # ========== 步骤1：获取区间内的所有交易 ==========
            trades_list = self._get_trades_in_interval(start_timestamp, end_timestamp)
            
            # ========== 步骤2：交易排序（开仓优先，避免平仓数量不足）==========
            def get_trade_priority(trade):
                """返回交易优先级，数字越小越优先"""
                dir_type = trade['dir']
                if dir_type in ['Open Long', 'Open Short']:
                    return 1
                elif dir_type in ['Short > Long', 'Long > Short']:
                    return 2
                elif dir_type in ['Close Long', 'Close Short']:
                    return 3
                elif dir_type in ['Auto-Deleveraging', 'Settlement'] or 'Liquidated' in dir_type:
                    return 4
                else:
                    return 5
            
            # 按币种分组，对每个币种的交易单独排序
            trades_by_coin = {}
            for trade in trades_list:
                coin = trade['coin']
                if coin not in trades_by_coin:
                    trades_by_coin[coin] = []
                trades_by_coin[coin].append(trade)
            
            sorted_trades_list = []
            for coin in sorted(trades_by_coin.keys()):
                coin_trades = trades_by_coin[coin]
                coin_trades_sorted = sorted(coin_trades, key=get_trade_priority)
                sorted_trades_list.extend(coin_trades_sorted)
            
            # ========== 步骤3：FIFO模拟所有交易，计算realized_pnl ==========
            total_realized_pnl = 0.0
            
            for trade in sorted_trades_list:
                coin = trade['coin']
                dir_type = trade['dir']
                amount = trade['amount']
                price = trade['price']
                side = trade.get('side', None)
                event_time = trade.get('time', '')  # 获取原始事件时间
                
                # 确保币种的队列存在
                if coin not in position_queues:
                    position_queues[coin] = []
                
                queue = position_queues[coin]
                pnl = 0.0
                
                # 根据交易类型处理（使用原始事件时间而不是区间时间）
                if dir_type == 'Open Long':
                    pnl = self._process_open_long(queue, amount, price)
                elif dir_type == 'Open Short':
                    pnl = self._process_open_short(queue, amount, price)
                elif dir_type == 'Close Long':
                    pnl = self._process_close_long(queue, amount, price, coin, event_time)
                elif dir_type == 'Close Short':
                    pnl = self._process_close_short(queue, amount, price, coin, event_time)
                elif dir_type == 'Short > Long':
                    pnl = self._process_short_to_long(queue, amount, price, coin)
                elif dir_type == 'Long > Short':
                    pnl = self._process_long_to_short(queue, amount, price)
                elif dir_type == 'Auto-Deleveraging':
                    pnl = self._process_auto_deleveraging(queue, amount, price, side)
                elif 'Liquidated' in dir_type:
                    pnl = self._process_liquidation(queue, amount, price, dir_type)
                elif dir_type == 'Settlement':
                    pnl = self._process_settlement(queue, amount, price)
                else:
                    if self.debug:
                        print(f"⚠️  警告: 未知的交易类型: {dir_type} (时间: {event_time})")
                
                total_realized_pnl += pnl
            
            # ========== 步骤4：提取队列持仓（只在有交易时记录）==========
            queue_positions_list = []
            queue_positions_str = ''
            
            # 🔍 关键修改：只有当区间内有交易时，才记录队列持仓
            if len(sorted_trades_list) > 0:
                for coin, queue in position_queues.items():
                    if not queue:
                        continue
                    
                    # 保留原始队列的每一笔持仓（不汇总）
                    for price, amount in queue:
                        if abs(amount) > 1e-10:  # 跳过数量为0的持仓
                            if amount > 0:
                                # 多头
                                queue_positions_list.append({
                                    'coin': coin,
                                    'amount': amount,
                                    'price': price,
                                    'dir': 'long'
                                })
                            else:
                                # 空头
                                queue_positions_list.append({
                                    'coin': coin,
                                    'amount': amount,  # 保持负数
                                    'price': price,
                                    'dir': 'short'
                                })
                
                # 格式化队列持仓字符串
                queue_positions_str = str(queue_positions_list).replace('"', "'") if queue_positions_list else ''
            
            # 调试信息：显示队列状态
            if len(queue_positions_list) > 0 and idx < 10:
                print(f"  ✅ 区间 {idx} ({self.intervals_df.iloc[idx]['time']}): 提取了 {len(queue_positions_list)} 笔队列持仓")
                for qpos in queue_positions_list[:3]:  # 只显示前3笔
                    print(f"     - {qpos['coin']} {qpos['dir']}: {qpos['amount']} @ {qpos['price']}")
            elif len(sorted_trades_list) > 0 and len(position_queues) > 0 and len(queue_positions_list) == 0 and idx < 10:
                # 🔍 只在有交易但队列为空时才警告（这才是真正的问题）
                print(f"  ⚠️  区间 {idx}: 有交易但队列为空！这不应该发生")
                print(f"     交易数: {len(sorted_trades_list)}")
                for coin, queue in list(position_queues.items())[:2]:
                    print(f"     - {coin} 队列长度: {len(queue)}, 内容: {queue[:2]}")
            
            # ========== 步骤5：从队列提取当前持仓并验证 ==========
            # 从 position_queues 构建当前持仓列表（用于验证和记录）
            current_perp_positions = []
            for coin, queue in position_queues.items():
                if not queue:
                    continue
                # 汇总每个币种的净持仓
                total_amount = sum(amount for _, amount in queue)
                if abs(total_amount) > 1e-10:
                    if total_amount > 0:
                        current_perp_positions.append({'coin': coin, 'amount': total_amount, 'dir': 'long'})
                    else:
                        current_perp_positions.append({'coin': coin, 'amount': total_amount, 'dir': 'short'})
            
            # 格式化为字符串
            current_perp_positions_str = str(current_perp_positions).replace('"', "'") if current_perp_positions else ''
            
            # 获取实际持仓（从positions_df）用于验证
            actual_perp_positions_str = self._find_position_before(end_timestamp, 'perp')
            actual_perp_positions = self._parse_perp_positions(actual_perp_positions_str)
            
            # 🔍 只有在有交易时才进行验证
            if len(sorted_trades_list) > 0 and len(queue_positions_list) > 0 and len(actual_perp_positions) > 0:
                # 获取区间结束时间（用于验证警告）
                interval_end_time = self.intervals_df.iloc[idx]['time']
                
                # 建立币种到交易时间的映射（用于警告信息）
                coin_last_trade_time = {}
                for trade in sorted_trades_list:
                    coin = trade['coin']
                    trade_time = trade.get('time', interval_end_time)
                    coin_last_trade_time[coin] = trade_time  # 记录最后一笔交易时间
                
                # 比较队列持仓和实际持仓（需要先汇总队列持仓）
                # 汇总队列持仓：按币种和方向分组
                queue_summary = {}
                for queue_pos in queue_positions_list:
                    coin = queue_pos['coin']
                    amount = queue_pos['amount']
                    dir_type = queue_pos['dir']
                    key = (coin, dir_type)
                    
                    if key not in queue_summary:
                        queue_summary[key] = 0
                    queue_summary[key] += amount
                
                # 验证1：检查 FIFO 队列中的币种是否在实际持仓中
                for (coin, queue_dir), queue_total in queue_summary.items():
                    # 在实际持仓中查找
                    found = False
                    for actual_pos in actual_perp_positions:
                        if actual_pos['coin'] == coin and actual_pos.get('dir') == queue_dir:
                            actual_amount = actual_pos['amount']
                            diff = abs(queue_total - actual_amount)
                            
                            if diff > 1e-6:
                                # 检查是否已经警告过这个币种+方向的不匹配
                                warning_key = (coin, queue_dir, 'mismatch')
                                if warning_key not in warned_mismatches:
                                    # 使用该币种最后一笔交易的时间
                                    last_trade_time = coin_last_trade_time.get(coin, interval_end_time)
                                    print(f"⚠️  警告: {coin} {queue_dir} 持仓不匹配！队列={queue_total:.8f}, 实际={actual_amount:.8f}, 差异={diff:.8f} (交易时间: {last_trade_time})")
                                    warned_mismatches.add(warning_key)
                            
                            found = True
                            break
                    
                    if not found and abs(queue_total) > 1e-6:
                        # 检查是否已经警告过这个币种+方向未找到
                        warning_key = (coin, queue_dir, 'not_found')
                        if warning_key not in warned_mismatches:
                            # 使用该币种最后一笔交易的时间
                            last_trade_time = coin_last_trade_time.get(coin, interval_end_time)
                            print(f"⚠️  警告: {coin} {queue_dir} 在实际持仓中未找到！队列={queue_total:.8f} (交易时间: {last_trade_time})")
                            warned_mismatches.add(warning_key)
                
                # 验证2：反向检查 - 实际持仓中的币种是否在 FIFO 队列中
                # 🔧 改进：只验证本区间有交易的币种，避免因价格列缺失导致的误报
                traded_coins = set(trade['coin'] for trade in sorted_trades_list)
                
                for actual_pos in actual_perp_positions:
                    coin = actual_pos['coin']
                    actual_dir = actual_pos.get('dir', 'long' if actual_pos['amount'] > 0 else 'short')
                    actual_amount = actual_pos['amount']
                    key = (coin, actual_dir)
                    
                    # 只验证本区间有交易的币种
                    if coin in traded_coins:
                        # 检查这个币种+方向是否在队列汇总中
                        if key not in queue_summary:
                            # 实际有持仓，但 FIFO 队列中没有！
                            if abs(actual_amount) > 1e-6:
                                warning_key = (coin, actual_dir, 'missing_in_queue')
                                if warning_key not in warned_mismatches:
                                    # 使用该币种最后一笔交易的时间
                                    last_trade_time = coin_last_trade_time.get(coin, interval_end_time)
                                    print(f"⚠️  警告: {coin} {actual_dir} 在实际持仓中存在（{actual_amount:.8f}），但FIFO队列中缺失！(交易时间: {last_trade_time})")
                                    warned_mismatches.add(warning_key)
            
            # ========== 步骤6：虚拟平仓计算virtual_pnl ==========
            # 🔍 新逻辑：因为每个区间都用区间开始价重新虚拟开仓
            # 所以 virtual_pnl = (区间结束价 - 区间开始价) × 持仓数量
            total_virtual_pnl = 0.0
            
            # 获取区间结束时的价格
            end_prices = {}
            for coin, queue in position_queues.items():
                if not queue:
                    continue
                
                price_col = f'{coin}_perp_price'
                if price_col in self.intervals_df.columns:
                    end_price = self.intervals_df.iloc[idx][price_col]
                    if end_price and end_price > 0:
                        end_prices[coin] = end_price
            
            # 计算虚拟盈亏
            for coin, queue in position_queues.items():
                if not queue or coin not in end_prices:
                    continue
                
                end_price = end_prices[coin]
                
                # 🔍 因为队列中所有持仓都是用同一个价格（区间开始价）虚拟开仓的
                # 所以直接用 (结束价 - 开仓价) × 数量
                for open_price, amount in queue:
                    if amount > 1e-10:  # 多头
                        virtual_pnl = (end_price - open_price) * amount
                    elif amount < -1e-10:  # 空头
                        virtual_pnl = (open_price - end_price) * abs(amount)
                    else:
                        virtual_pnl = 0.0
                    
                    total_virtual_pnl += virtual_pnl
            
            # ========== 步骤7：获取区间内的资产变化（资金费率等）==========
            total_perp_asset_change = self._get_perp_asset_changes_in_interval(start_timestamp, end_timestamp)
            
            # ========== 步骤8：计算当前区间的合约账户价值 ==========
            current_account_value = prev_account_value + total_realized_pnl + total_virtual_pnl + total_perp_asset_change
            
            # ========== 步骤9：更新当前区间的数据 ==========
            self.intervals_df.at[idx, 'perp_positions'] = current_perp_positions_str  # 使用FIFO计算后的持仓
            self.intervals_df.at[idx, 'perp_queue_positions'] = queue_positions_str
            self.intervals_df.at[idx, 'perp_account_value'] = current_account_value
            self.intervals_df.at[idx, 'realized_pnl'] = total_realized_pnl
            self.intervals_df.at[idx, 'virtual_pnl'] = total_virtual_pnl
            
            # 显示进度（每1000个区间或最后一个）
            if (idx + 1) % 1000 == 0 or (idx + 1) == total_intervals:
                progress_pct = ((idx + 1) / total_intervals) * 100
                print(f"  已处理 {idx + 1}/{total_intervals} 个区间 ({progress_pct:.1f}%)...", flush=True)
        
        print(f"\n✅ 合约账户价值计算完成！", flush=True)
        
        # 统计信息
        total_trades = 0
        for idx in range(1, len(self.intervals_df)):
            start_ts = int(self.intervals_df.iloc[idx-1]['timestamp'])
            end_ts = int(self.intervals_df.iloc[idx]['timestamp'])
            trades = self._get_trades_in_interval(start_ts, end_ts)
            total_trades += len(trades)
        
        print(f"   统计期间合约交易总数: {total_trades}", flush=True)
        
        # 检查是否有队列数据
        non_empty_queues = len(self.intervals_df[self.intervals_df['perp_queue_positions'] != ''])
        print(f"   有队列持仓的区间数: {non_empty_queues}/{len(self.intervals_df)}")
        
        # 显示持仓不匹配汇总
        if warned_mismatches:
            print(f"\n   ⚠️  持仓验证警告汇总:")
            mismatch_warnings = [w for w in warned_mismatches if w[2] == 'mismatch']
            not_found_warnings = [w for w in warned_mismatches if w[2] == 'not_found']
            
            if mismatch_warnings:
                print(f"      持仓数量不匹配: {len(mismatch_warnings)} 个")
                for coin, dir_type, _ in mismatch_warnings:
                    print(f"        - {coin} {dir_type}")
            
            if not_found_warnings:
                print(f"      持仓未找到: {len(not_found_warnings)} 个")
                for coin, dir_type, _ in not_found_warnings:
                    print(f"        - {coin} {dir_type}")
        
        if total_trades == 0:
            print(f"   ⚠️  警告: 整个统计期间没有发现任何合约交易！")
        
        return True

    def calculate_net_value(self) -> bool:
        """
        步骤7：计算净值
        
        逻辑：
        1. 计算总资产 = spot_account_value + perp_account_value
        2. 初始化总份额：第一个非零 total_assets 时，总份额 = 总资产，净值 = 1.0
        3. 份额变化：解析 share_change 字段（格式：5.0/current_net_value）
           - 份额变化量 = share_change 数值 / 当前净值
           - 总份额 = 上一行的份额 + 份额变化量
        4. 净值 = 总资产 / 总份额
        
        返回:
            bool: 是否成功计算
        """
        print("\n" + "="*80, flush=True)
        print("步骤7：计算净值", flush=True)
        print("="*80, flush=True)
        
        if self.intervals_df is None:
            print("❌ 尚未生成时间区间", flush=True)
            return False
        
        total_intervals = len(self.intervals_df)
        print(f"开始处理 {total_intervals} 个区间...\n", flush=True)
        
        # 添加列
        self.intervals_df['total_assets'] = 0.0
        self.intervals_df['total_shares'] = 0.0
        self.intervals_df['net_value'] = 0.0
        self.intervals_df['share_change'] = ''
        self.intervals_df['cumulative_pnl'] = 0.0  # 累计PnL
        
        # 步骤1：计算总资产
        for idx in range(total_intervals):
            spot_value = float(self.intervals_df.at[idx, 'spot_account_value'])
            perp_value = float(self.intervals_df.at[idx, 'perp_account_value'])
            total_assets = spot_value + perp_value
            self.intervals_df.at[idx, 'total_assets'] = total_assets
        
        print(f"✅ 总资产计算完成", flush=True)
        
        # 步骤2：找到第一个非零的 total_assets，初始化总份额
        first_non_zero_idx = None
        for idx in range(len(self.intervals_df)):
            total_assets = self.intervals_df.at[idx, 'total_assets']
            if abs(total_assets) > 1e-10:
                first_non_zero_idx = idx
                # 初始化：总份额 = 总资产，净值 = 1.0
                self.intervals_df.at[idx, 'total_shares'] = total_assets
                self.intervals_df.at[idx, 'net_value'] = 1.0
                
                # 计算初始累计PnL：从第一个事件到当前区间的所有closedPnl
                initial_timestamp = int(self.intervals_df.at[idx, 'timestamp'])
                mask_initial = self.positions_df['timestamp'] <= initial_timestamp
                initial_events = self.positions_df[mask_initial]
                
                initial_cumulative_pnl = 0.0
                for _, event in initial_events.iterrows():
                    closed_pnl_str = event.get('closedPnl', '')
                    if closed_pnl_str and closed_pnl_str != '':
                        try:
                            initial_cumulative_pnl += float(closed_pnl_str)
                        except (ValueError, TypeError):
                            pass
                
                self.intervals_df.at[idx, 'cumulative_pnl'] = initial_cumulative_pnl
                
                print(f"✅ 初始化总份额", flush=True)
                print(f"   首次非零资产区间: {self.intervals_df.at[idx, 'time']}", flush=True)
                print(f"   初始总资产: ${total_assets:,.2f}", flush=True)
                print(f"   初始总份额: {total_assets:,.2f}", flush=True)
                print(f"   初始净值: 1.0", flush=True)
                print(f"   初始累计PnL: ${initial_cumulative_pnl:,.2f}\n", flush=True)
                break
        
        if first_non_zero_idx is None:
            print("⚠️  警告: 所有区间的总资产都为0", flush=True)
            return True
        
        # 步骤3：从第二个区间开始，计算份额和净值
        print(f"开始计算份额和净值...\n", flush=True)
        for idx in range(first_non_zero_idx + 1, len(self.intervals_df)):
            interval_timestamp = int(self.intervals_df.at[idx, 'timestamp'])
            
            # 获取当前区间的总资产
            current_total_assets = float(self.intervals_df.at[idx, 'total_assets'])
            
            # 获取上一个区间的总份额、净值和累计PnL
            prev_total_shares = float(self.intervals_df.at[idx - 1, 'total_shares'])
            prev_net_value = float(self.intervals_df.at[idx - 1, 'net_value'])
            prev_cumulative_pnl = float(self.intervals_df.at[idx - 1, 'cumulative_pnl'])
            
            # 查找该区间内是否有 share_change
            # 从 positions_df 中查找时间戳在上一区间和当前区间之间的记录
            prev_timestamp = int(self.intervals_df.at[idx - 1, 'timestamp'])
            
            mask = (self.positions_df['timestamp'] > prev_timestamp) & (self.positions_df['timestamp'] <= interval_timestamp)
            interval_events = self.positions_df[mask]
            
            # 累计份额变化和closedPnl
            total_share_change = 0.0
            share_change_strs = []
            total_closed_pnl = 0.0
            
            for _, event in interval_events.iterrows():
                # 处理 share_change
                share_change_str = event.get('share_change', '')
                if share_change_str and share_change_str != '':
                    share_change_strs.append(share_change_str)
                    
                    # 解析 share_change（格式：5.0/current_net_value 或 -5.0/current_net_value）
                    try:
                        # 提取数值部分（斜杠之前）
                        if '/' in share_change_str:
                            value_str = share_change_str.split('/')[0].strip()
                            change_value = float(value_str)
                            
                            # 份额变化量 = 数值 / 上一区间的净值
                            # 注意：这里用上一区间的净值来计算份额变化
                            if abs(prev_net_value) > 1e-10:
                                share_delta = change_value / prev_net_value
                                total_share_change += share_delta
                            else:
                                if self.debug:
                                    print(f"⚠️  警告: 区间 {idx} 的上一净值为0，无法计算份额变化")
                        else:
                            if self.debug:
                                print(f"⚠️  警告: share_change 格式不正确: {share_change_str}")
                    
                    except Exception as e:
                        if self.debug:
                            print(f"⚠️  警告: 解析 share_change 失败: {share_change_str}, 错误: {e}")
                
                # 累计 closedPnl
                closed_pnl_str = event.get('closedPnl', '')
                if closed_pnl_str and closed_pnl_str != '':
                    try:
                        total_closed_pnl += float(closed_pnl_str)
                    except (ValueError, TypeError):
                        pass
            
            # 计算当前总份额
            current_total_shares = prev_total_shares + total_share_change
            
            # 计算当前净值
            if abs(current_total_shares) > 1e-10:
                current_net_value = current_total_assets / current_total_shares
            else:
                current_net_value = 0.0
            
            # 计算累计PnL：上一行的累计PnL + 当前区间的closedPnl
            current_cumulative_pnl = prev_cumulative_pnl + total_closed_pnl
            
            # 保存结果
            self.intervals_df.at[idx, 'total_shares'] = current_total_shares
            self.intervals_df.at[idx, 'net_value'] = current_net_value
            self.intervals_df.at[idx, 'cumulative_pnl'] = current_cumulative_pnl
            if share_change_strs:
                self.intervals_df.at[idx, 'share_change'] = '; '.join(share_change_strs)
            
            # 显示进度（每1000个区间）
            if (idx + 1) % 1000 == 0:
                progress_pct = ((idx + 1) / len(self.intervals_df)) * 100
                print(f"  已处理 {idx + 1}/{len(self.intervals_df)} 个区间 ({progress_pct:.1f}%)...", flush=True)
        
        print(f"\n✅ 净值计算完成！", flush=True)
        
        # 显示统计信息
        if len(self.intervals_df) > first_non_zero_idx:
            first_value = self.intervals_df.iloc[first_non_zero_idx]['net_value']
            last_value = self.intervals_df.iloc[-1]['net_value']
            first_assets = self.intervals_df.iloc[first_non_zero_idx]['total_assets']
            last_assets = self.intervals_df.iloc[-1]['total_assets']
            first_cumulative_pnl = self.intervals_df.iloc[first_non_zero_idx]['cumulative_pnl']
            last_cumulative_pnl = self.intervals_df.iloc[-1]['cumulative_pnl']
            
            print(f"\n   初始净值: {first_value:.6f}", flush=True)
            print(f"   最终净值: {last_value:.6f}", flush=True)
            if abs(first_value) > 1e-10:
                return_rate = (last_value - first_value) / first_value * 100
                print(f"   收益率: {return_rate:+.2f}%", flush=True)
            
            print(f"\n   初始总资产: ${first_assets:,.2f}", flush=True)
            print(f"   最终总资产: ${last_assets:,.2f}", flush=True)
            
            print(f"\n   初始累计PnL: ${first_cumulative_pnl:,.2f}", flush=True)
            print(f"   最终累计PnL: ${last_cumulative_pnl:,.2f}", flush=True)
            print(f"   累计PnL增长: ${last_cumulative_pnl - first_cumulative_pnl:+,.2f}", flush=True)
            
            # 统计份额变化次数
            share_change_count = len(self.intervals_df[self.intervals_df['share_change'] != ''])
            print(f"\n   有份额变化的区间数: {share_change_count}/{len(self.intervals_df)}", flush=True)
        
        return True

def main():
    """主函数 - 测试"""
    # ==================== 配置参数 ====================
    ADDRESS = "0x0000000afcd4de376f2bf0094cdd01712f125995"
    INTERVAL = '1h'  # 时间区间
    DEBUG = False
    
    # ==================================================
    
    try:
        # 创建计算器（从 API 获取数据）
        calculator = NetValueCalculatorV2(
            address=ADDRESS,
            interval=INTERVAL,
            debug=DEBUG
        )
        
        # 初始化（完成前4步）
        if not calculator.initialize():
            print("\n❌ 初始化失败")
            return
        
        # 步骤5：计算现货账户价值
        if not calculator.calculate_spot_account_value():
            print("\n❌ 步骤5失败")
            return
        
        print("\n✅ 步骤5完成！")
        
        # 步骤6：计算合约账户价值
        if not calculator.calculate_perp_account_value():
            print("\n❌ 步骤6失败")
            return
        
        print("\n✅ 步骤6完成！")
        
        # 步骤7：计算净值
        if not calculator.calculate_net_value():
            print("\n❌ 步骤7失败")
            return
        
        print("\n✅ 步骤7完成！")
        
        print("\n" + "="*80)
        print("✅ 所有步骤完成！")
        print("="*80)
        
        # 显示结果
        print(f"\n可以查看 calculator.intervals_df 来查看结果")
        print(f"列: {calculator.intervals_df.columns.tolist()}")
        
        # 显示最终净值信息
        if len(calculator.intervals_df) > 0:
            last_row = calculator.intervals_df.iloc[-1]
            print(f"\n最终状态:")
            print(f"   时间: {last_row['time']}")
            print(f"   总资产: ${last_row['total_assets']:,.2f}")
            print(f"   总份额: {last_row['total_shares']:,.6f}")
            print(f"   净值: {last_row['net_value']:.6f}")
            print(f"   累计PnL: ${last_row['cumulative_pnl']:,.2f}")

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
