# -*- coding: utf-8 -*-
"""
逐笔撤销事件，计算每个事件前的持仓状态（基于快照）

功能：
1. 从 API 加载历史快照数据（现货 + 合约）
2. 按时间分组快照（精确到秒）
3. 从 API 读取事件数据（使用 EventImpactRecorder）
4. 将快照插入到对应的事件位置
5. 从最新快照开始逐笔向前撤销事件
6. 遇到快照时校验计算持仓与快照持仓是否一致
7. 输出包含每笔事件前后持仓的CSV文件

撤销逻辑：
- 现货：先用 before_spot_trade 替换交易币种，再减去 spot_position_changes，再减去 asset_change
- 合约：用 before_perp_trade 完全替换同币种的所有持仓（多空都替换）

快照校验：
- 撤销到有快照的事件时，比较计算持仓与快照持仓
- 如果不一致，警告并使用快照数据替换（快照为准）

使用方法：
只需提供账户地址，程序会自动：
- 从 API 获取快照数据
- 从 API 获取事件数据
- 从最新快照开始向前计算每笔事件后的持仓状态
- 校验并修正持仓计算误差
"""

import sys
import os
import json
import pandas as pd
from typing import Dict, Any, List

# 添加模块路径
script_dir = os.path.dirname(os.path.abspath(__file__))

# 使用相对导入（同一包内的模块）
from .event_impact_recorder import EventImpactRecorder
from .data_loader import DataLoader
from datetime import datetime


class PositionBackwardCalculator:
    """持仓反向计算器（逐笔撤销）"""
    
    def __init__(self, address: str, export_csv: bool = False):
        """
        初始化
        
        参数:
            address: 账户地址
            export_csv: 是否导出CSV文件
        """
        self.address = address
        self.export_csv = export_csv
        
    def load_snapshots_from_api(self) -> Dict[int, Dict]:
        """
        从 API 加载快照数据并按时间分组
        
        返回:
            Dict[timestamp, snapshot]: 按时间戳分组的快照数据
            格式: {
                timestamp_ms: {
                    'spot_positions': {'BTC': 10.5, 'USDC': 5000},
                    'perp_positions': [{'coin': 'ETH', 'amount': -453.45, 'dir': 'short'}]
                }
            }
        """
        print("\n" + "="*80)
        print("步骤1：从 API 加载快照数据")
        print("="*80)
        
        try:
            loader = DataLoader()
            snapshots_data = loader.load_snapshots_from_api(self.address)
            
            if not snapshots_data:
                print("❌ 加载快照数据失败")
                return {}
            
            # 按时间分组快照
            grouped_snapshots = self._group_snapshots_by_time(snapshots_data)
            
            if not grouped_snapshots:
                print("❌ 没有有效的快照数据")
                return {}
            
            print(f"✅ 成功加载快照数据")
            
            return grouped_snapshots
            
        except Exception as e:
            print(f"[ERROR] 加载快照失败: {e}")
            return {}
    
    def _group_snapshots_by_time(self, snapshots_data: Dict) -> Dict[int, Dict]:
        """
        按 account_summary 的时间点分组快照数据
        
        新逻辑：
        1. 使用 account_summary 中的 snapshot_time 作为快照时间点
        2. 将 positions 和 spot_balances 关联到对应的时间点
        3. 如果某个时间点没有 positions 或 spot_balances，则为空（表示无持仓）
        
        参数:
            snapshots_data: API 返回的快照数据
        
        返回:
            Dict[timestamp, snapshot]: 分组后的快照
        """
        from datetime import datetime
        import calendar
        
        # 存储分组后的快照
        grouped = {}
        
        # 步骤1：从 account_summary 获取所有快照时间点
        snapshot_times = set()
        for summary in snapshots_data.get('account_summary', []):
            time_str = summary.get('snapshot_time', '')
            if not time_str:
                continue
            
            timestamp_ms = self._parse_snapshot_time(time_str)
            if timestamp_ms is not None:
                snapshot_times.add(timestamp_ms)
        
        
        # 步骤2：处理 positions（合约持仓），按时间分组
        positions_by_time = {}
        for pos in snapshots_data.get('positions', []):
            time_str = pos.get('snapshot_time', '')
            if not time_str:
                continue
            
            timestamp_ms = self._parse_snapshot_time(time_str)
            if timestamp_ms is None:
                continue
            
            if timestamp_ms not in positions_by_time:
                positions_by_time[timestamp_ms] = []
            
            # 转换格式
            size = float(pos.get('size', 0))
            positions_by_time[timestamp_ms].append({
                'coin': pos.get('coin', ''),
                'amount': size,
                'dir': 'long' if size > 0 else 'short' if size < 0 else ''
            })
        
        # 步骤3：处理 spot_balances（现货余额），按时间分组
        spot_by_time = {}
        for balance in snapshots_data.get('spot_balances', []):
            time_str = balance.get('snapshot_time', '')
            if not time_str:
                continue
            
            timestamp_ms = self._parse_snapshot_time(time_str)
            if timestamp_ms is None:
                continue
            
            if timestamp_ms not in spot_by_time:
                spot_by_time[timestamp_ms] = {}
            
            coin = balance.get('coin', '')
            amount = float(balance.get('total_amount', 0))
            
            # 只保留金额大于0的持仓
            if amount > 1e-10:
                spot_by_time[timestamp_ms][coin] = amount
        
        # 步骤4：使用 account_summary 的时间点作为主键，关联持仓数据
        # 即使某个时间点没有持仓数据，也保留（表示该时间点无持仓）
        for timestamp_ms in snapshot_times:
            grouped[timestamp_ms] = {
                'spot_positions': spot_by_time.get(timestamp_ms, {}),
                'perp_positions': positions_by_time.get(timestamp_ms, [])
            }
        
        return grouped
    
    def _parse_snapshot_time(self, time_str: str) -> int:
        """
        解析快照时间字符串为毫秒时间戳（精确到毫秒）
        
        参数:
            time_str: 时间字符串，如 "2025-08-17 05:52:34.123456+0000"
        
        返回:
            int: 毫秒时间戳（毫秒级精度）
        """
        try:
            import calendar
            # 移除时区后缀
            time_str_clean = time_str.replace('+0000', '').strip()
            
            # 分离秒和微秒部分
            if '.' in time_str_clean:
                main_part, micro_part = time_str_clean.split('.')
                # 解析主时间部分
                dt = datetime.strptime(main_part, '%Y-%m-%d %H:%M:%S')
                # 解析微秒部分（取前6位，转换为毫秒）
                micro_str = micro_part[:6].ljust(6, '0')  # 补齐到6位
                microseconds = int(micro_str)
                milliseconds = microseconds // 1000  # 微秒转毫秒
            else:
                # 没有微秒部分
                dt = datetime.strptime(time_str_clean, '%Y-%m-%d %H:%M:%S')
                milliseconds = 0
            
            # 转换为毫秒时间戳（使用 calendar.timegm 确保 UTC）
            timestamp_ms = int(calendar.timegm(dt.timetuple()) * 1000) + milliseconds
            return timestamp_ms
        except Exception as e:
            print(f"⚠️  警告: 解析快照时间失败: {time_str}, 错误: {e}")
            return None
    
    def parse_position_changes(self, changes_str: str) -> Dict:
        """解析持仓变化字符串"""
        if not changes_str or changes_str == '':
            return {}
        
        try:
            # 处理字符串格式：{'BTC': 100, 'ETH': -20}
            changes_str_normalized = changes_str.replace("'", '"')
            
            # 尝试解析为字典
            changes_dict = json.loads(changes_str_normalized)
            
            # 统一格式：如果是数字，转换为 {'change': value}
            result = {}
            for coin, value in changes_dict.items():
                if isinstance(value, dict):
                    result[coin] = value.get('change', 0)
                else:
                    result[coin] = value
            
            return result
        except Exception as e:
            print(f"⚠️  警告: 解析持仓变化失败: {changes_str[:50]}... 错误: {e}")
            return {}
    
    def parse_before_trade(self, before_str: str) -> Dict:
        """解析交易前持仓字符串"""
        if not before_str or before_str == '':
            return {}
        
        try:
            # 处理字符串格式
            before_str_normalized = before_str.replace("'", '"')
            before_dict = json.loads(before_str_normalized)
            return before_dict
        except Exception as e:
            print(f"⚠️  警告: 解析交易前持仓失败: {before_str[:50]}... 错误: {e}")
            return {}
    
    def parse_perp_position_changes(self, changes_str: str) -> Dict:
        """
        解析合约持仓变化字符串（保留完整结构）
        
        输入格式：
            "{'BTC': 'amount': 5, 'price': 50000, 'dir': Open Long, 'side': B}"
        
        返回格式：
            {'BTC': {'amount': 5, 'price': 50000, 'dir': 'Open Long', 'side': 'B'}}
        """
        if not changes_str or changes_str == '':
            return {}
        
        try:
            # 处理字符串格式：将单引号替换为双引号
            changes_str_normalized = changes_str.replace("'", '"')
            
            # 尝试解析为字典
            changes_dict = json.loads(changes_str_normalized)
            
            return changes_dict
        except Exception as e:
            # JSON 解析失败，尝试手动解析
            # 格式可能是：{'BTC': 'amount': 5, 'price': 50000, 'dir': Open Long, 'side': B}
            try:
                import re
                result = {}
                
                # 匹配币种和其内容
                # 格式：'COIN': 'amount': X, 'price': Y, 'dir': Z, 'side': W
                pattern = r"['\"](\w+)['\"]:\s*['\"]?amount['\"]?:\s*([\d.]+),\s*['\"]?price['\"]?:\s*([\d.]+),\s*['\"]?dir['\"]?:\s*([^,}]+),\s*['\"]?side['\"]?:\s*([BA])"
                matches = re.findall(pattern, changes_str)
                
                for match in matches:
                    coin, amount, price, dir_val, side = match
                    result[coin] = {
                        'amount': float(amount),
                        'price': float(price),
                        'dir': dir_val.strip().strip("'\""),
                        'side': side.strip().strip("'\"")
                    }
                
                if result:
                    return result
                    
            except Exception:
                pass
            
            # 如果都失败了，返回空字典
            return {}
    
    def undo_spot_event(self, current_positions: Dict, position_changes: Dict, asset_change: float = 0.0) -> Dict:
        """
        撤销现货事件
        
        逻辑：
        1. 减去 spot_position_changes（包含交易币种和USDC的完整变化）
        2. 减去 spot_asset_change_ex_position（USDC手续费，仅当feeToken==USDC时有值）
        
        参数:
            current_positions: 当前持仓 {'BTC': 10.99, 'USDC': 10000}
            position_changes: 持仓变化 {'BTC': 10, 'USDC': -500000}
            asset_change: 资产变化（spot_asset_change_ex_position），默认0.0
        
        返回:
            撤销后的持仓
        """
        # 复制当前持仓
        next_positions = current_positions.copy()
        
        # 步骤1：减去 position_changes
        for coin, change in position_changes.items():
            current_amount = next_positions.get(coin, 0)
            next_amount = current_amount - change
            
            if abs(next_amount) < 1e-10:
                # 数量为0，删除该币种
                if coin in next_positions:
                    del next_positions[coin]
            else:
                next_positions[coin] = next_amount
        
        # 步骤2：减去 asset_change（USDC手续费）
        if abs(asset_change) > 1e-10:
            usdc_amount = next_positions.get('USDC', 0)
            next_usdc = usdc_amount - asset_change
            
            if abs(next_usdc) < 1e-10:
                if 'USDC' in next_positions:
                    del next_positions['USDC']
            else:
                next_positions['USDC'] = next_usdc
        
        return next_positions
    
    def undo_perp_event(self, current_positions: List, position_changes: Dict) -> List:
        """
        撤销合约事件
        
        逻辑：
        根据 position_changes 中的 side 和 amount 进行撤销：
        - side = 'B'（买入）：撤销后持仓 = 当前持仓 - amount
        - side = 'A'（卖出）：撤销后持仓 = 当前持仓 + amount
        
        参数:
            current_positions: 当前持仓列表
                [{'coin': 'BTC', 'amount': 10, 'dir': 'long'}, ...]
            position_changes: 持仓变化
                {'BTC': {'amount': 5, 'price': 50000, 'dir': 'Open Long', 'side': 'B'}}
        
        返回:
            撤销后的持仓列表
        """
        # 如果 position_changes 为空，直接返回当前持仓
        if not position_changes:
            return [pos.copy() for pos in current_positions]
        
        # 将当前持仓转换为字典，方便查找和修改
        positions_dict = {}
        for pos in current_positions:
            positions_dict[pos['coin']] = pos['amount']
        
        # 遍历每个持仓变化
        for coin, info in position_changes.items():
            sz = info.get('amount', 0)
            side = info.get('side', '')
            
            # 获取当前该币种的持仓
            current_amount = positions_dict.get(coin, 0)
            
            # 根据 side 进行撤销
            if side == 'B':
                # 买入的撤销 = 减去
                new_amount = current_amount - sz
            elif side == 'A':
                # 卖出的撤销 = 加回
                new_amount = current_amount + sz
            else:
                # side 无效，打印警告并跳过
                print(f"⚠️  警告: 合约事件 side 字段无效，跳过撤销！")
                print(f"    币种: {coin}, side: '{side}', amount: {sz}")
                print(f"    当前持仓: {current_amount}")
                continue
            
            # 更新持仓字典
            if abs(new_amount) < 1e-10:
                # 持仓为0，删除该币种
                if coin in positions_dict:
                    del positions_dict[coin]
            else:
                positions_dict[coin] = new_amount
        
        # 将字典转换回列表格式
        next_positions = []
        for coin, amount in positions_dict.items():
            if amount > 0:
                direction = 'long'
            elif amount < 0:
                direction = 'short'
            else:
                continue  # 跳过持仓为0的
            
            next_positions.append({
                'coin': coin,
                'amount': amount,
                'dir': direction
            })
        
        return next_positions
    
    def format_spot_positions(self, positions: Dict) -> str:
        """格式化现货持仓为字符串"""
        if not positions:
            return ''
        
        items = []
        for coin, amount in sorted(positions.items()):
            formatted_amount = f"{amount:.10f}".rstrip('0').rstrip('.')
            items.append(f"'{coin}': {formatted_amount}")
        
        return '{' + ', '.join(items) + '}'
    
    def format_perp_positions(self, positions: List) -> str:
        """格式化合约持仓为字符串"""
        if not positions:
            return ''
        
        items = []
        for pos in sorted(positions, key=lambda x: x['coin']):
            coin = pos['coin']
            amount = pos['amount']
            direction = pos['dir']
            
            formatted_amount = f"{amount:.10f}".rstrip('0').rstrip('.')
            items.append(f"{{'coin': '{coin}', 'amount': {formatted_amount}, 'dir': '{direction}'}}")
        
        return '[' + ', '.join(items) + ']'

    def _format_dict(self, data: Dict) -> str:
        """
        格式化字典为字符串
        
        用于将 before_spot_trade, before_perp_trade, spot_position_changes 等转换为字符串
        """
        if not data:
            return ''
        
        # 直接使用 json.dumps
        return json.dumps(data, ensure_ascii=False)
    
    def _insert_snapshots_to_events(self, df: pd.DataFrame, snapshots: Dict[int, Dict]) -> pd.DataFrame:
        """
        将快照插入到事件DataFrame中
        
        新逻辑：
        1. 对于每个事件，找到"时间大于该事件、小于等于下一个事件"的快照
        2. 如果有多个快照，选择离下一个事件时间最近的那个（即时间最大的）
        3. 将快照插入到下一个事件的行（代表撤销该事件后的持仓）
        4. 添加 is_snapshot_recorded 列标记是否有快照
        
        例如：事件 11.1、11.5，快照 11.2、11.3、11.4
        → 选择 11.4 的快照，插入到 11.5 的行
        
        参数:
            df: 事件DataFrame（按时间倒序，最新在前）
            snapshots: 分组后的快照数据
        
        返回:
            pd.DataFrame: 添加了 spot_snapshot, perp_snapshot, is_snapshot_recorded 列的DataFrame
        """
        print("\n" + "="*80)
        print("步骤3：将快照插入到事件")
        print("="*80)
        
        # 添加快照列
        df['spot_snapshot'] = None
        df['perp_snapshot'] = None
        df['is_snapshot_recorded'] = False  # 新增：标记是否有快照记录
        
        # 确保 timestamp 是数值类型
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        
        if len(snapshots) == 0:
            print("   ⚠️ 没有快照数据")
            return df
        
        # 获取所有快照时间（排序）
        snapshot_times = sorted(snapshots.keys())
        print(f"   快照时间范围: {datetime.fromtimestamp(snapshot_times[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')} ~ "
              f"{datetime.fromtimestamp(snapshot_times[-1] / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取所有事件时间戳（df 是倒序的，所以事件从新到旧）
        event_timestamps = df['timestamp'].tolist()
        
        # 为每个事件找到对应的快照
        # 逻辑：对于事件 E_i（时间 T_i），找到在 (T_{i+1}, T_i] 范围内的快照
        # 注意：df 是倒序的，所以 i+1 是更早的事件
        inserted_count = 0
        skipped_snapshots = []
        
        for idx in range(len(df)):
            event_time = event_timestamps[idx]
            
            # 获取前一个事件的时间（更早的事件）
            if idx < len(df) - 1:
                prev_event_time = event_timestamps[idx + 1]
            else:
                prev_event_time = 0  # 最早的事件，没有更早的了
            
            # 找到在 (prev_event_time, event_time) 范围内的快照（严格小于）
            # 即：快照时间 > 前一个事件时间 且 快照时间 < 当前事件时间
            # 注意：排除 snap_time == event_time 的情况，因为无法确定先后顺序
            matching_snapshots = []
            for snap_time in snapshot_times:
                if prev_event_time < snap_time < event_time:
                    matching_snapshots.append(snap_time)
            
            if matching_snapshots:
                # 如果有多个快照，选择时间最大的（离当前事件最近的）
                selected_snapshot_time = max(matching_snapshots)
                snapshot_data = snapshots[selected_snapshot_time]
                
                # 插入快照数据
                row_idx = df.index[idx]
                df.at[row_idx, 'spot_snapshot'] = snapshot_data['spot_positions']
                df.at[row_idx, 'perp_snapshot'] = snapshot_data['perp_positions']
                df.at[row_idx, 'is_snapshot_recorded'] = True
                
                inserted_count += 1
                
                # 记录被跳过的快照
                if len(matching_snapshots) > 1:
                    for skip_time in matching_snapshots:
                        if skip_time != selected_snapshot_time:
                            skipped_snapshots.append(skip_time)
                
        
        print(f"\n✅ 成功插入 {inserted_count}/{len(snapshots)} 个快照")
        
        if skipped_snapshots:
            print(f"   ⚠️ 跳过 {len(skipped_snapshots)} 个快照（同一区间内有更新的快照）")
        
        # 检查是否有快照之后的事件
        latest_snapshot_time = max(snapshot_times)
        events_after_snapshot = df[df['timestamp'] > latest_snapshot_time]
        if len(events_after_snapshot) > 0:
            latest_snapshot_dt = datetime.fromtimestamp(latest_snapshot_time / 1000)
            print(f"\n⚠️  警告: 发现 {len(events_after_snapshot)} 个事件发生在最新快照之后")
            print(f"   最新快照时间: {latest_snapshot_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   这些事件将不会被处理（暂时忽略）")
        
        # 检查是否有快照早于所有事件
        earliest_event_time = min(event_timestamps)
        snapshots_before_all = [t for t in snapshot_times if t <= earliest_event_time]
        if snapshots_before_all:
            print(f"\n⚠️  警告: 发现 {len(snapshots_before_all)} 个快照早于所有事件")
        
        return df
    
    def _is_within_tolerance(self, calc_amount: float, snap_amount: float, 
                              abs_tol: float = 0.01, rel_tol: float = 0.01) -> bool:
        """
        判断计算值与快照值是否在容忍范围内
        
        规则：绝对误差 ≤ abs_tol 或 相对误差 ≤ rel_tol，满足任一即通过
        
        参数:
            calc_amount: 计算值
            snap_amount: 快照值（基准值）
            abs_tol: 绝对误差阈值，默认 0.01
            rel_tol: 相对误差阈值，默认 1%
        
        返回:
            bool: 是否在容忍范围内
        """
        diff = abs(calc_amount - snap_amount)
        
        # 条件1：绝对误差 ≤ 0.01
        if diff <= abs_tol:
            return True
        
        # 条件2：相对误差 ≤ 1%（需要快照不为0）
        if abs(snap_amount) > 1e-10:
            relative_error = diff / abs(snap_amount)
            if relative_error <= rel_tol:
                return True
        
        return False
    
    def _compare_positions(self, calculated: Dict, snapshot: Dict, position_type: str) -> tuple:
        """
        比较计算持仓和快照持仓
        
        使用相对误差（以快照值为基准），阈值 1%
        
        返回:
            (is_match: bool, differences: List[str])
        """
        differences = []
        
        if position_type == 'spot':
            # 现货持仓比较（字典格式）
            all_coins = set(calculated.keys()) | set(snapshot.keys())
            
            for coin in all_coins:
                calc_amount = calculated.get(coin, 0)
                snap_amount = snapshot.get(coin, 0)
                
                # 使用相对误差判断
                if not self._is_within_tolerance(calc_amount, snap_amount):
                    diff = calc_amount - snap_amount
                    # 计算相对误差百分比
                    if abs(snap_amount) > 1e-10:
                        rel_err = abs(diff) / abs(snap_amount) * 100
                        differences.append(
                            f"{coin}: 计算={calc_amount:.8f}, 快照={snap_amount:.8f}, "
                            f"差异={diff:.8f} ({rel_err:.2f}%)"
                        )
                    else:
                        differences.append(
                            f"{coin}: 计算={calc_amount:.8f}, 快照={snap_amount:.8f}, "
                            f"差异={diff:.8f}"
                        )
        
        elif position_type == 'perp':
            # 合约持仓比较（列表格式）
            # 按币种分组
            calc_by_coin = {pos['coin']: pos for pos in calculated}
            snap_by_coin = {pos['coin']: pos for pos in snapshot}
            
            all_coins = set(calc_by_coin.keys()) | set(snap_by_coin.keys())
            
            for coin in all_coins:
                calc_pos = calc_by_coin.get(coin, {'amount': 0, 'dir': ''})
                snap_pos = snap_by_coin.get(coin, {'amount': 0, 'dir': ''})
                
                calc_amount = calc_pos.get('amount', 0)
                snap_amount = snap_pos.get('amount', 0)
                
                # 使用相对误差判断
                if not self._is_within_tolerance(calc_amount, snap_amount):
                    diff = calc_amount - snap_amount
                    # 计算相对误差百分比
                    if abs(snap_amount) > 1e-10:
                        rel_err = abs(diff) / abs(snap_amount) * 100
                        differences.append(
                            f"{coin}: 计算={calc_amount:.8f} ({calc_pos.get('dir', '')}), "
                            f"快照={snap_amount:.8f} ({snap_pos.get('dir', '')}), "
                            f"差异={diff:.8f} ({rel_err:.2f}%)"
                        )
                    else:
                        differences.append(
                            f"{coin}: 计算={calc_amount:.8f} ({calc_pos.get('dir', '')}), "
                            f"快照={snap_amount:.8f} ({snap_pos.get('dir', '')}), "
                            f"差异={diff:.8f}"
                        )
        
        return len(differences) == 0, differences
    
    def calculate_backward(self, output_csv_path: str = None):
        """
        逐笔撤销事件，计算持仓（基于快照）
        
        参数:
            output_csv_path: 输出CSV文件路径（可选，仅当export_csv=True时需要）
        
        返回:
            pd.DataFrame: 包含持仓信息的DataFrame，按时间戳正序排列
                         如果失败则返回None
        """
        # 步骤1：加载快照
        snapshots = self.load_snapshots_from_api()
        if not snapshots:
            print("❌ 无法加载快照数据")
            return None
        
        # 找到最新的快照
        latest_snapshot_time = max(snapshots.keys())
        latest_snapshot = snapshots[latest_snapshot_time]
        
        print("\n" + "="*80)
        print("步骤2：获取事件数据")
        print("="*80)
        
        # 使用 EventImpactRecorder 获取事件数据
        # 注意：EventImpactRecorder 的 __init__ 会自动调用 load_data() 和 build_timeline()
        recorder = EventImpactRecorder(address=self.address)

        # 处理所有事件
        recorder.process_all_events()

        # 检查是否有事件数据
        if not recorder.impacts or len(recorder.impacts) == 0:
            print("❌ 没有事件数据")
            return None
        
        # 将 impacts 转换为 DataFrame
        impacts_data = []
        for impact in recorder.impacts:
            raw = impact.get('raw_data', {})
            impacts_data.append({
                'event_number': impact.get('event_number', ''),
                'time': impact.get('event_time_str', ''),
                'timestamp': impact.get('event_time', ''),
                'event_category': impact.get('event_type', ''),
                'event_type': impact.get('event_subtype', ''),
                'closedPnl': raw.get('closedPnl', ''),
                'before_spot_trade': self._format_dict(impact.get('before_spot_trade', {})),
                'before_perp_trade': self._format_dict(impact.get('before_perp_trade', {})),
                'spot_position_changes': self._format_dict(impact.get('spot_position_changes', {})),
                'spot_asset_change_ex_position': impact.get('spot_asset_change_ex_position', ''),
                'perp_position_changes': self._format_dict(impact.get('perp_position_changes', {})),
                'perp_asset_change_ex_position': impact.get('perp_asset_change_ex_position', ''),
                'share_change': impact.get('share_change', ''),
            })
        
        df = pd.DataFrame(impacts_data)
        
        # 添加持仓列
        df['spot_positions'] = ''
        df['perp_positions'] = ''
        
        # 插入快照
        df = self._insert_snapshots_to_events(df, snapshots)
        
        print("\n" + "="*80)
        print("步骤4：逐笔撤销事件，计算持仓（从最新快照开始）")
        print("="*80)
        
        # 找到第一个有快照记录的行（即最新的快照点）
        # 使用 is_snapshot_recorded 来判断
        start_idx = 0
        for idx in range(len(df)):
            if df.iloc[idx]['is_snapshot_recorded']:
                start_idx = idx
                break
        else:
            # 如果没有找到任何快照记录，使用 latest_snapshot_time 作为备用
            for idx in range(len(df)):
                if df.iloc[idx]['timestamp'] <= latest_snapshot_time:
                    start_idx = idx
                    break
        
        # 初始化当前持仓（使用起始行的快照数据）
        # 快照代表的是"撤销该事件后的持仓"
        if df.iloc[start_idx]['is_snapshot_recorded']:
            # 从该行的快照获取初始持仓
            initial_spot_snapshot = df.iloc[start_idx]['spot_snapshot']
            initial_perp_snapshot = df.iloc[start_idx]['perp_snapshot']
            current_spot = initial_spot_snapshot.copy() if initial_spot_snapshot else {}
            current_perp = [pos.copy() for pos in initial_perp_snapshot] if initial_perp_snapshot else []
        else:
            # 备用：使用最新快照的持仓
            current_spot = latest_snapshot['spot_positions'].copy()
            current_perp = [pos.copy() for pos in latest_snapshot['perp_positions']]
        
        if start_idx > 0:
            print(f"\n⚠️  跳过 {start_idx} 个最新快照之后的事件")
        
        # 统计校验情况
        total_snapshots_checked = 0
        snapshots_matched = 0
        snapshots_mismatched = 0
        
        # ========== 处理第一行（start_idx）：不撤销，直接记录 ==========
        # 因为 current_spot 就是"撤销该事件后的持仓"，直接记录
        first_row_idx = df.index[start_idx]
        df.at[first_row_idx, 'spot_positions'] = self.format_spot_positions(current_spot)
        df.at[first_row_idx, 'perp_positions'] = self.format_perp_positions(current_perp)
        
        # ========== 从第二行开始正常处理 ==========
        for idx in range(start_idx + 1, len(df)):
            # 当前行的索引
            row_idx = df.index[idx]
            
            # 获取事件信息
            spot_position_changes = self.parse_position_changes(df.at[row_idx, 'spot_position_changes'])
            perp_position_changes = self.parse_perp_position_changes(df.at[row_idx, 'perp_position_changes'])
            
            # 获取现货资产变化（手续费等）
            spot_asset_change_str = df.at[row_idx, 'spot_asset_change_ex_position']
            try:
                spot_asset_change = float(spot_asset_change_str) if spot_asset_change_str and spot_asset_change_str != '' else 0.0
            except (ValueError, TypeError):
                spot_asset_change = 0.0

            # 1. 先撤销事件，得到事件前的持仓
            current_spot = self.undo_spot_event(current_spot, spot_position_changes, spot_asset_change)
            current_perp = self.undo_perp_event(current_perp, perp_position_changes)
            
            # 2. 记录持仓（这是事件发生前的持仓）
            df.at[row_idx, 'spot_positions'] = self.format_spot_positions(current_spot)
            df.at[row_idx, 'perp_positions'] = self.format_perp_positions(current_perp)
            
            # 3. 撤销后校验（快照代表的是撤销该事件后的持仓）
            has_snapshot = df.at[row_idx, 'is_snapshot_recorded']
            
            if has_snapshot:
                total_snapshots_checked += 1
                
                # 获取快照数据（即使为空也是有效的）
                spot_snapshot = df.at[row_idx, 'spot_snapshot']
                perp_snapshot = df.at[row_idx, 'perp_snapshot']
                
                # 如果快照数据为 None，视为空持仓
                if spot_snapshot is None:
                    spot_snapshot = {}
                if perp_snapshot is None:
                    perp_snapshot = []
                
                # 比较现货持仓
                spot_match, spot_diffs = self._compare_positions(
                    current_spot, spot_snapshot, 'spot'
                )
                
                # 比较合约持仓
                perp_match, perp_diffs = self._compare_positions(
                    current_perp, perp_snapshot, 'perp'
                )
                
                if spot_match and perp_match:
                    # 持仓一致（在相对误差 1% 内）
                    snapshots_matched += 1
                    # 显示持仓是否为空
                    status = ""
                    if len(spot_snapshot) == 0 and len(perp_snapshot) == 0:
                        status = " [无持仓]"
                    print(f"\n  ✅ 快照校验通过 (事件 #{df.at[row_idx, 'event_number']}, "
                          f"{df.at[row_idx, 'time']}){status}")
                else:
                    # 持仓不一致（超过相对误差 1%）
                    snapshots_mismatched += 1
                    print(f"\n  ⚠️  快照校验失败 (事件 #{df.at[row_idx, 'event_number']}, "
                          f"{df.at[row_idx, 'time']})")
                    
                    if not spot_match:
                        print(f"     【现货不一致】")
                        for diff in spot_diffs:
                            print(f"       - {diff}")
                    
                    if not perp_match:
                        print(f"     【合约不一致】")
                        for diff in perp_diffs:
                            print(f"       - {diff}")
                    
                # 无论校验是否通过，都用快照数据替换（防止误差累积）
                current_spot = spot_snapshot.copy()
                current_perp = [pos.copy() for pos in perp_snapshot]
            
            # 显示进度
            if (idx - start_idx) % 100000 == 0:
                print(f"  已处理 {idx - start_idx}/{len(df) - start_idx - 1} 笔事件...")
        
        print(f"\n✅ 完成！共处理 {len(df) - start_idx} 笔事件")
        
        # 显示校验统计
        if total_snapshots_checked > 0:
            print(f"\n📊 快照校验统计:")
            print(f"   总快照数: {total_snapshots_checked}")
            print(f"   校验通过: {snapshots_matched} ({snapshots_matched/total_snapshots_checked*100:.1f}%)")
            print(f"   校验失败: {snapshots_mismatched} ({snapshots_mismatched/total_snapshots_checked*100:.1f}%)")
        else:
            print(f"\n⚠️  没有快照需要校验")
        
        # 按时间戳正序排列（从小到大，方便后续计算净值）
        print("\n" + "="*80)
        print("步骤5：按时间戳正序排列")
        print("="*80)
        
        # 确保时间戳列是数值类型
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        
        # 直接反转 DataFrame（原始是倒序，反转后变正序）
        # 注意：不使用 sort_values，因为同时间戳的事件顺序也需要反转
        df = df.iloc[::-1].reset_index(drop=True)
        
        # 重新排列列顺序：将相关字段放在一起
        column_order = [
            'event_number',
            'time',
            'timestamp',
            'event_category',
            'event_type',
            'closedPnl',
            'spot_position_changes',
            'spot_asset_change_ex_position',
            'spot_positions',
            'spot_snapshot',
            'perp_position_changes',
            'perp_asset_change_ex_position',
            'perp_positions',
            'perp_snapshot',
            'is_snapshot_recorded',
            'share_change'
        ]
        df = df[column_order]
        
        # 导出CSV（可选）
        if self.export_csv:
            print("\n" + "="*80)
            print("步骤6：导出结果")
            print("="*80)
            
            # 确保时间戳列导出时保持完整精度（不使用科学计数法）
            df['timestamp'] = df['timestamp'].astype('int64')
            
            if output_csv_path:
                df.to_csv(output_csv_path, encoding='utf-8-sig', index=False)
                print(f"✅ 结果已保存到: {output_csv_path}")
            else:
                print("⚠️  警告: 未指定输出路径，跳过CSV导出")
        else:
            print("\n" + "="*80)
            print("步骤6：跳过CSV导出（export_csv=False）")
            print("="*80)
        
        return df


def main():
    """主函数"""
    # 强制使用 UTF-8 输出（避免 Windows gbk 编码问题）
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    # 配置参数
    ADDRESS = "0x0000000afcd4de376f2bf0094cdd01712f125995"
    EXPORT_CSV = True

    address_prefix = ADDRESS[:10] if len(ADDRESS) >= 10 else ADDRESS
    output_dir = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_CSV = os.path.join(output_dir, f"{address_prefix}_positions_backward.csv")
    
    try:
        calculator = PositionBackwardCalculator(ADDRESS, export_csv=EXPORT_CSV)
        df_result = calculator.calculate_backward(OUTPUT_CSV if EXPORT_CSV else None)
        
        if df_result is not None:
            print(f"完成: {len(df_result)} 笔事件 -> {OUTPUT_CSV}")
        
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    main()
