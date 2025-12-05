"""
事件影响记录器 - Event Impact Recorder
=====================================

完全独立的工具类，用于记录和分析每个交易事件对账户的影响
适用于基于份额法的净值曲线计算

主要功能：
1. 解析交易历史JSON文件（支持Hyperliquid格式）
2. 记录每个原始事件的持仓变化、资产变化和份额变化
3. 支持多种事件类型：合约交易、现货交易、资金费、充提、转账等
4. 区分现货账户和合约账户的资产变化
5. 导出详细的CSV报告供后续分析

核心概念：
- 持仓变化：记录币种数量的变化（合约：正数=多头，负数=空头）
- 除持仓影响的资产变化：分为现货资产变化和合约资产变化
- 份额变化：只有充值、提现、外部转账等操作影响份额
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List
from copy import deepcopy


class EventImpactRecorder:
    """
    事件影响记录器类 - 完全独立版本
    
    使用方法：
    --------
    recorder = EventImpactRecorder(
        json_file_path='path/to/account_data.json',
        account_address='0x...'
    )
    recorder.process_all_events()
    recorder.export_to_csv('output.csv')
    
    输出字段：
    --------
    - 时间序号/时间/时间戳：事件发生时间
    - 事件大类/事件类型：事件分类信息
    - fee/feeToken/closedPnl/usdc/usdcValue：原始交易数据
    - toPerp/user/destination：转账方向信息
    - 现货持仓变化/合约持仓变化：持仓数量变化（字典格式）
    - 除持仓影响的现货资产变化：现货资产变化（不含持仓价值变化）
    - 除持仓影响的合约资产变化：合约资产变化（不含持仓价值变化）
    - 总份额变化：份额公式（如 usdc/current_net_value）
    
    支持的事件类型：
    --------------
    Trade: perps(合约交易), spot(现货交易)
    Funding: funding(资金费)
    Ledger: deposit, withdraw, accountClassTransfer, spotTransfer, 
            internalTransfer, subAccountTransfer, vaultCreate, vaultDeposit, 
            vaultWithdraw, vaultDistribution, spotGenesis, send, 
            cStakingTransfer, liquidation, rewardsClaim, activateDexAbstraction,
            accountActivationGas
    """
    
    def __init__(self, address: str = None):
        """
        初始化记录器
        
        Args:
            address: 账户地址（必填）
        """
        self.account_address = address.lower() if address else None
        
        self.data = None
        self.timeline = []
        self.impacts = []  # 存储所有事件的影响记录
        
        if address:
            self.load_data()
            self.extract_account_address()
            self.build_timeline()
    
    # ==================== 数据加载和处理 ====================
    
    def load_data(self):
        """从数据库加载数据并构建 self.data 结构"""
        import sys
        import os
        
        # 尝试相对导入（当作为包的一部分导入时）
        # 如果失败，则使用绝对导入（当直接运行时）
        try:
            from .data_loader import DataLoader
        except ImportError:
            # 相对导入失败，使用绝对导入
            try:
                from data_loader import DataLoader
            except ImportError:
                # 如果还是失败，尝试从 main 包导入
                from main.data_loader import DataLoader
        
        try:
            # 使用 DataLoader 加载数据（从 API）
            loader = DataLoader()
            self.data = loader.load_all_events(self.account_address)
            
        except Exception as e:
            print(f"[ERROR] 数据加载失败: {e}")
            raise
    
    def extract_account_address(self):
        """提取账户地址（data_loader.py 返回的统一格式：顶层 address 字段）"""
        # data_loader.py 返回的格式固定为：{'address': ..., 'data': {...}}
        self.account_address = self.data.get('address', '').lower()
    
    def build_timeline(self):
        """构建事件时间线"""
        data = self.data.get('data', {})
        
        # 1. 添加 trades（data_loader.py 返回的统一格式：直接是列表）
        trades = data.get('trade', [])
        for trade in trades:
            trade_copy = deepcopy(trade)
            
            # 处理 coin 字段：去掉 /USDC 后缀
            if 'coin' in trade_copy and isinstance(trade_copy['coin'], str):
                if trade_copy['coin'].endswith('/USDC'):
                    trade_copy['coin'] = trade_copy['coin'].replace('/USDC', '')
            
            self.timeline.append({
                'time': trade_copy.get('time'),
                'type': 'trade',
                'subtype': trade_copy.get('type'),  # 'perps' or 'spot'
                'data': trade_copy
            })
        
        # 2. 添加 funding（data_loader.py 返回的统一格式：直接是列表）
        funding_list = data.get('funding', [])
        for funding in funding_list:
            self.timeline.append({
                'time': funding.get('time'),
                'type': 'funding',
                'data': funding
            })
        
        # 3. 添加 ledger（data_loader.py 返回的统一格式：包装在 data 中）
        ledger_raw = data.get('ledger', {})
        ledger_data = ledger_raw.get('data', []) if isinstance(ledger_raw, dict) else []
        
        for ledger in ledger_data:
            delta = ledger.get('delta', {})
            self.timeline.append({
                'time': ledger.get('time'),
                'type': 'ledger',
                'subtype': delta.get('type'),
                'data': ledger
            })
        
        # 按时间倒序排列（从新到旧）
        self.timeline.sort(key=lambda x: x['time'], reverse=True)
    
    # ==================== 主处理方法 ====================
    
    def process_all_events(self):
        """
        处理所有事件，记录每个事件的影响
        """
        self.impacts = []
        
        for i, event in enumerate(self.timeline, 1):
            impact = self.record_event_impact(event)
            
            # 添加事件序号和时间信息（北京时间）
            impact['event_number'] = i
            impact['event_time'] = event['time']
            impact['event_time_str'] = self._to_beijing_time_str(event['time'])
            
            self.impacts.append(impact)
            
            if i % 100000 == 0:
                print(f"  已处理 {i}/{len(self.timeline)} 个事件...")
        
        return self.impacts
    
    def record_event_impact(self, event: Dict) -> Dict:
        """
        记录单个事件的影响
        
        Args:
            event: 事件数据
        
        Returns:
            影响记录字典
        """
        event_type = event['type']
        
        if event_type == 'trade':
            return self.record_trade_impact(event)
        elif event_type == 'funding':
            return self.record_funding_impact(event)
        elif event_type == 'ledger':
            return self.record_ledger_impact(event)
        else:
            return self._empty_impact(event)
    
    # ==================== Trade 交易影响 ====================
    
    def record_trade_impact(self, event: Dict) -> Dict:
        """记录交易事件的影响"""
        trade_data = event['data']
        trade_type = event['subtype']  # 'perp' or 'spot'
        
        # 兼容两种格式：'perp'/'perps' 和 'spot'
        if trade_type in ['perp', 'perps']:
            return self.record_perp_trade_impact(trade_data)
        elif trade_type == 'spot':
            return self.record_spot_trade_impact(trade_data)
        else:
            return self._empty_impact(event)
    
    def is_perp_open_trade(self, trade: Dict) -> bool:
        """
        判断合约交易是否为开仓
        
        使用双重判断机制：
        1. 优先检查 closedPnl 是否为零（开仓不产生实现盈亏）
        2. 再检查 dir 字段是否为开仓类型
        
        Args:
            trade: 交易记录字典
        
        Returns:
            True表示开仓交易，False表示平仓交易
        """
        # 第一道防线：如果有非零的 closedPnl，一定不是开仓
        closed_pnl = trade.get('closedPnl', '0')
        try:
            if float(closed_pnl) != 0.0:
                return False  # 有实现盈亏，说明是平仓而非开仓
        except (ValueError, TypeError):
            pass
        
        # 第二道防线：检查 dir 字段是否为开仓类型
        dir_val = trade.get('dir', '')
        # 合约：Open Long 或 Open Short
        return dir_val in ['Open Long', 'Open Short']
    
    def record_perp_trade_impact(self, trade: Dict) -> Dict:
        """
        记录合约交易的影响
        
        持仓变化记录：
        - 包含交易数量(amount)、交易价格(price)、交易方向(dir)
        
        除持仓影响的合约资产变化：
        - feeToken == 'USDC' 时：-fee（统一扣除手续费）
        - feeToken != 'USDC' 时：打印警告，fee不记录（设为0）
        """
        coin = trade.get('coin')
        side = trade.get('side')  # 'B' for buy, 'A' for sell
        sz = float(trade.get('sz', 0))
        px = float(trade.get('px', 0))  # 交易价格
        start_position = float(trade.get('startPosition', 0))
        closed_pnl = float(trade.get('closedPnl', 0))
        fee = float(trade.get('fee', 0))
        fee_token = trade.get('feeToken', '')
        trade_dir = trade.get('dir', '')  # 交易方向
        trade_type = trade.get('type', 'perp')  # 获取交易类型，默认为'perp'
        
        # 检查 feeToken 并决定是否记录手续费
        if fee_token != 'USDC':
            print(
                f"[警告] 合约交易的 feeToken 不是 USDC，手续费不记录！\n"
                f"  币种: {coin}\n"
                f"  feeToken: {fee_token}\n"
                f"  交易数量: {sz}\n"
                f"  交易价格: {px}\n"
                f"  手续费: {fee}\n"
                f"  交易方向: {trade_dir}"
            )
            # feeToken 不是 USDC 时，不记录手续费
            perp_asset_change_ex_position = 0
        else:
            # feeToken 是 USDC，记录手续费
            perp_asset_change_ex_position = -fee
        
        # 检查 side 参数是否有效
        if side not in ['B', 'A']:
            raise ValueError(
                f"[错误] 合约交易的 side 参数无效！\n"
                f"  币种: {coin}\n"
                f"  side: {side} (必须是 'B' 或 'A')\n"
                f"  交易数量: {sz}\n"
                f"  起始持仓: {start_position}"
            )
        
        # 计算交易后的持仓
        # side='B' (买入): 增加持仓 (开多或平空)
        # side='A' (卖出): 减少持仓 (开空或平多)
        if side == 'B':
            end_position = start_position + sz
        else:  # side == 'A'
            end_position = start_position - sz
        
        # 记录交易前的合约持仓
        # 根据 start_position 判断方向
        if start_position > 0:
            position_dir = 'long'
        elif start_position < 0:
            position_dir = 'short'
        else:
            position_dir = ''  # 持仓为0时，方向为空
        
        before_perp_trade = {
            'coin': coin,
            'amount': start_position,  
            'dir': position_dir
        }
        
        # 构建持仓变化字典（直接记录交易信息）
        perp_position_changes = {
            coin: {
                'amount': sz,           # 交易数量
                'price': px,            # 交易价格
                'dir': trade_dir,       # 交易方向（如 'Open Long', 'Close Short', 'Auto-Deleveraging' 等）
                'side': side            # 交易侧（'B'=买入, 'A'=卖出）
            }
        }
        
        return {
            'event_type': 'trade',
            'event_subtype': trade_type,  # 使用原始的type值（'perp'或'perps'）
            'coin': coin,
            
            # 交易前持仓
            'before_spot_trade': {},
            'before_perp_trade': before_perp_trade,
            
            # 持仓变化（记录原操作的影响）
            'spot_position_changes': {},
            'perp_position_changes': perp_position_changes,
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,  # 合约交易不影响现货资产
            'perp_asset_change_ex_position': perp_asset_change_ex_position,  # -fee
            
            # 份额变化（只有存取款影响份额）
            'share_change': 0,
            
            # 原始数据
            'raw_data': {
                'oid': trade.get('oid', ''),
                'side': side,
                'sz': sz,
                'px': px,
                'dir': trade_dir,
                'fee': fee,
                'feeToken': fee_token,
                'closedPnl': closed_pnl,
                'startPosition': start_position,
                'endPosition': end_position,
            }
        }
    
    def record_spot_trade_impact(self, trade: Dict) -> Dict:
        """
        记录现货交易的影响
        
        持仓变化（spot_position_changes）：
        - 交易币种：买入增加（+sz），卖出减少（-sz）
        - USDC：买入减少（-sz*px），卖出增加（+sz*px）
        - 如果feeToken是交易币种，则该币种持仓变化需要减去手续费
        
        除持仓影响的现货资产变化：
        - feeToken == 'USDC' 时：-fee
        - feeToken == coin 时：0（已体现在持仓变化中）
        - feeToken 是其他币种时：0（已体现在持仓变化中）
        """
        coin = trade.get('coin')
        side = trade.get('side')  # 'B' or 'A'
        sz = float(trade.get('sz', 0))
        px = float(trade.get('px', 0))
        fee = float(trade.get('fee', 0))
        fee_token = trade.get('feeToken', '')
        closed_pnl = float(trade.get('closedPnl', 0))
        start_position = float(trade.get('startPosition', 0))
        
        # 检查 side 参数是否有效
        if side not in ['B', 'A']:
            raise ValueError(
                f"[错误] 现货交易的 side 参数无效！\n"
                f"  币种: {coin}\n"
                f"  side: {side} (必须是 'B' 或 'A')\n"
                f"  交易数量: {sz}\n"
                f"  交易价格: {px}\n"
                f"  手续费: {fee}"
            )
        
        # 初始化持仓变化字典
        spot_position_changes = {}
        
        if side == 'B':
            # 买入：增加币（正数），减少USDC
            coin_change = sz
            usdc_change = -(sz * px)
        else:  # side == 'A'
            # 卖出：减少币（负数），增加USDC
            coin_change = -sz
            usdc_change = sz * px
        
        # 处理手续费和持仓变化
        if fee_token == 'USDC':
            # feeToken 是 USDC：手续费影响资产，不影响币种持仓
            spot_position_changes[coin] = {'change': coin_change}
            spot_position_changes['USDC'] = {'change': usdc_change}
            spot_asset_change_ex_position = -fee
        elif fee_token == coin:
            # feeToken 和交易币种一致：手续费从该币种扣除
            # 币种持仓变化需要减去手续费
            spot_position_changes[coin] = {'change': coin_change - fee}
            spot_position_changes['USDC'] = {'change': usdc_change}
            spot_asset_change_ex_position = 0
        else:
            # feeToken 是其他币种（既不是 USDC 也不是交易币种）
            # 币种持仓变化不受手续费影响
            spot_position_changes[coin] = {'change': coin_change}
            spot_position_changes['USDC'] = {'change': usdc_change}
            # 在持仓变化中扣除该币种的手续费
            spot_position_changes[fee_token] = {'change': -fee}
            spot_asset_change_ex_position = 0
        
        return {
            'event_type': 'trade',
            'event_subtype': 'spot',
            'coin': coin,
            'side': side,
            
            # 交易前持仓（不再使用）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（记录原操作的影响）
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': spot_asset_change_ex_position,
            'perp_asset_change_ex_position': 0,  # 现货交易不影响合约资产
            
            # 份额变化（只有存取款影响份额）
            'share_change': 0,
            
            # 原始数据
            'raw_data': {
                'oid': trade.get('oid', ''),
                'fee': fee,
                'feeToken': fee_token,
                'sz': sz,
                'px': px,
                'closedPnl': closed_pnl,
                'startPosition': start_position,
            }
        }
    
    # ==================== Funding 资金费影响 ====================
    
    def record_funding_impact(self, event: Dict) -> Dict:
        """
        记录资金费率事件的影响
        
        资金费不影响持仓，只影响资产
        
        除持仓影响的合约资产变化：
        - 支付资金费（usdc < 0）：perp_asset_change_ex_position = usdc（负数）
        - 收到资金费（usdc > 0）：perp_asset_change_ex_position = usdc（正数）
        """
        funding_data = event['data']
        delta = funding_data.get('delta', {})
        usdc_change = float(delta.get('usdc', 0))
        coin = funding_data.get('coin', '')
        
        # 资金费直接影响合约资产
        # usdc为负数表示支付，正数表示收到
        perp_asset_change_ex_position = usdc_change
        
        return {
            'event_type': 'funding',
            'event_subtype': 'funding',
            'coin': coin,
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（资金费不影响持仓）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,  # 资金费不影响现货资产
            'perp_asset_change_ex_position': perp_asset_change_ex_position,
            
            # 份额变化（只有存取款影响份额）
            'share_change': 0,
            
            # 原始数据
            'raw_data': {
                'usdc': usdc_change,
            }
        }
    
    # ==================== Ledger 账本影响 ====================
    
    def record_ledger_impact(self, event: Dict) -> Dict:
        """记录ledger事件的影响"""
        ledger_data = event['data']
        delta = ledger_data.get('delta', {})
        ledger_type = event['subtype']
        
        # 根据ledger类型调用相应方法
        # 注意：spotGenesis 需要传递完整的事件数据（包含时间戳）来获取价格
        method_map = {
            'deposit': self.record_deposit_impact,
            'withdraw': self.record_withdraw_impact,
            'accountClassTransfer': self.record_account_class_transfer_impact,
            'spotTransfer': self.record_spot_transfer_impact,
            'internalTransfer': self.record_internal_transfer_impact,
            'subAccountTransfer': self.record_subaccount_transfer_impact,
            'vaultCreate': self.record_vault_create_impact,
            'vaultDeposit': self.record_vault_deposit_impact,
            'vaultWithdraw': self.record_vault_withdraw_impact,
            'vaultDistribution': self.record_vault_distribution_impact,
            'spotGenesis': self.record_spot_genesis_impact,
            'send': self.record_send_impact,
            'cStakingTransfer': self.record_staking_transfer_impact,
            'liquidation': self.record_liquidation_impact,
            'rewardsClaim': self.record_rewards_claim_impact,
            'activateDexAbstraction': self.record_activate_dex_abstraction_impact,
            'accountActivationGas': self.record_account_activation_gas_impact,
        }
        
        method = method_map.get(ledger_type)
        if method:
            # spotGenesis 和 rewardsClaim 需要额外传递完整的事件数据（用于获取价格）
            if ledger_type in ['spotGenesis', 'rewardsClaim']:
                return method(delta, event_data=ledger_data)
            else:
                return method(delta)
        else:
            # 未知的ledger类型，抛出错误
            raise ValueError(
                f"[错误] 遇到未知的 ledger 类型！\n"
                f"  ledger类型: {ledger_type}\n"
                f"  支持的类型: {list(method_map.keys())}\n"
                f"  事件数据: {delta}"
            )
    
    def record_deposit_impact(self, delta: Dict) -> Dict:
        """
        记录充值
        
        资产变化：
        - perp_asset_change_ex_position: +usdc
        
        份额变化：
        - share_change: usdc/current_net_value（字符串公式）
        """
        usdc = float(delta.get('usdc', 0))
        
        # 份额变化：输出为字符串公式
        share_change = f"{usdc}/current_net_value"
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'deposit',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（充值不直接影响持仓）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': usdc,  # 充值增加合约资产
            
            # 份额变化（字符串公式）
            'share_change': share_change,  # "usdc/current_net_value"
            
            # 原始数据
            'raw_data': {'usdc': usdc}
        }
    
    def record_withdraw_impact(self, delta: Dict) -> Dict:
        """
        记录提现
        
        资产变化：
        - perp_asset_change_ex_position: -(usdc + fee)
        
        份额变化：
        - share_change: -usdc/current_net_value（字符串公式）
        """
        usdc = float(delta.get('usdc', 0))
        fee = float(delta.get('fee', 0))
        fee_token = delta.get('feeToken', '')
        
        # 资产变化：提现减少资产，需要扣除手续费
        perp_asset_change_ex_position = -(usdc + fee)
        
        # 份额变化：输出为字符串公式
        share_change = f"-{usdc}/current_net_value"
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'withdraw',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（提现不直接影响持仓）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': perp_asset_change_ex_position,  # -(usdc + fee)
            
            # 份额变化（字符串公式）
            'share_change': share_change,  # "-usdc/current_net_value"
            
            # 原始数据
            'raw_data': {'usdc': usdc, 'fee': fee, 'feeToken': fee_token}
        }
    
    def record_account_class_transfer_impact(self, delta: Dict) -> Dict:
        """
        记录账户类别转账（perp <-> spot 之间的 USDC 转账）
        
        资产和持仓变化：
        - toPerp=True (Spot -> Perp):
          * perp_asset_change_ex_position: +usdc
          * spot_position_changes: {'USDC': -usdc}
        - toPerp=False (Perp -> Spot):
          * perp_asset_change_ex_position: -usdc
          * spot_position_changes: {'USDC': usdc}
        """
        usdc = float(delta.get('usdc', 0))
        to_perp = delta.get('toPerp', False)
        
        # 根据转账方向设置资产和持仓变化
        if to_perp:
            # Spot -> Perp：现货转到合约
            perp_asset_change_ex_position = usdc  # 合约资产增加
            spot_position_changes = {
                'USDC': {
                    'change': -usdc  # 现货USDC减少
                }
            }
        else:
            # Perp -> Spot：合约转到现货
            perp_asset_change_ex_position = -usdc  # 合约资产减少
            spot_position_changes = {
                'USDC': {
                    'change': usdc  # 现货USDC增加
                }
            }
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'accountClassTransfer',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': perp_asset_change_ex_position,
            
            # 份额变化（内部转账不影响份额）
            'share_change': 0,
            
            # 原始数据
            'raw_data': {'usdc': usdc, 'toPerp': to_perp}
        }
    
    def record_spot_transfer_impact(self, delta: Dict) -> Dict:
        """
        记录现货转账（记录原操作的影响）
        
        资产和份额变化：
        - 转出现货到其他地址（user=本账户）：
          * spot_position_changes: {'token': -amount}
          * spot_asset_change_ex_position: -fee
          * share_change: -(usdcValue+fee)/current_net_value
        - 从其他地址转入现货（destination=本账户）：
          * spot_position_changes: {'token': amount}
          * share_change: usdcValue/current_net_value
        """
        token = delta.get('token', '')
        amount = float(delta.get('amount', 0))
        usdcValue = float(delta.get('usdcValue', 0))
        user = delta.get('user', '').lower()
        destination = delta.get('destination', '').lower()
        fee = float(delta.get('fee', 0))
        fee_token = delta.get('feeToken', '')
        
        spot_position_changes = {}
        spot_asset_change_ex_position = 0
        share_change = 0
        
        if self.account_address:
            if user == self.account_address:
                # 转出现货到其他地址
                spot_position_changes = {
                    token: {
                        'change': -amount  # 原操作：转出（负数）
                    }
                }
                spot_asset_change_ex_position = -fee  # 手续费影响资产
                # 份额减少：计算实际数值
                share_numerator = -(usdcValue + fee)
                share_change = f"{share_numerator}/current_net_value"
                
            elif destination == self.account_address:
                # 从其他地址转入现货
                spot_position_changes = {
                    token: {
                        'change': amount  # 原操作：转入（正数）
                    }
                }
                spot_asset_change_ex_position = 0  # 转入无手续费
                share_change = f"{usdcValue}/current_net_value"  # 份额增加
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'spotTransfer',
            'coin': token,
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': spot_asset_change_ex_position,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（字符串公式）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {
                'token': token,
                'amount': amount,
                'usdcValue': usdcValue,
                'fee': fee,
                'feeToken': fee_token,
                'user': user,
                'destination': destination,
            }
        }
    
    def record_internal_transfer_impact(self, delta: Dict) -> Dict:
        """
        记录内部转账（USDC）
        
        资产和份额变化：
        - 转出 Spot USDC 到其他地址（user=本账户）：
          * spot_position_changes: {'USDC': -usdc}
          * spot_asset_change_ex_position: -fee
          * share_change: -(usdc+fee)/current_net_value
        - 从其他地址转入 Spot USDC（destination=本账户）：
          * spot_position_changes: {'USDC': usdc}
          * share_change: usdc/current_net_value
        """
        usdc = float(delta.get('usdc', 0))
        user = delta.get('user', '').lower()
        destination = delta.get('destination', '').lower()
        fee = float(delta.get('fee', 0))
        fee_token = delta.get('feeToken', '')
        
        spot_position_changes = {}
        spot_asset_change_ex_position = 0
        share_change = 0
        
        if self.account_address:
            if user == self.account_address:
                # 转出 Spot USDC 到其他地址
                spot_position_changes = {
                    'USDC': {
                        'change': -usdc  # 现货USDC减少
                    }
                }
                spot_asset_change_ex_position = -fee  # 手续费影响资产
                # 份额减少：计算实际数值
                share_numerator = -(usdc + fee)
                share_change = f"{share_numerator}/current_net_value"
                
            elif destination == self.account_address:
                # 从其他地址转入 Spot USDC
                spot_position_changes = {
                    'USDC': {
                        'change': usdc  # 现货USDC增加
                    }
                }
                spot_asset_change_ex_position = 0  # 转入无手续费
                share_change = f"{usdc}/current_net_value"  # 份额增加
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'internalTransfer',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': spot_asset_change_ex_position,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（字符串公式）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {'usdc': usdc, 'fee': fee, 'feeToken': fee_token, 'user': user, 'destination': destination}
        }
    
    def record_subaccount_transfer_impact(self, delta: Dict) -> Dict:
        """
        记录子账户转账（USDC）
        
        资产和份额变化：
        - 主账户转出到子账户（user=本账户）：
          * spot_position_changes: {'USDC': -usdc}
          * share_change: -usdc/current_net_value
        - 子账户转入到主账户（destination=本账户）：
          * spot_position_changes: {'USDC': usdc}
          * share_change: usdc/current_net_value
        """
        usdc = float(delta.get('usdc', 0))
        user = delta.get('user', '').lower()
        destination = delta.get('destination', '').lower()
        
        spot_position_changes = {}
        share_change = 0
        
        if self.account_address:
            if user == self.account_address:
                # 主账户转出到子账户
                spot_position_changes = {
                    'USDC': {
                        'change': -usdc  # 现货USDC减少
                    }
                }
                share_change = f"-{usdc}/current_net_value"  # 份额减少
                
            elif destination == self.account_address:
                # 子账户转入到主账户
                spot_position_changes = {
                    'USDC': {
                        'change': usdc  # 现货USDC增加
                    }
                }
                share_change = f"{usdc}/current_net_value"  # 份额增加
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'subAccountTransfer',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（字符串公式）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {'usdc': usdc, 'user': user, 'destination': destination}
        }
    
    def record_vault_deposit_impact(self, delta: Dict) -> Dict:
        """
        记录金库存款
        
        资产和份额变化：
        - perp_asset_change_ex_position: -usdc（合约账户USDC减少）
        - share_change: -usdc/current_net_value（份额减少）
        
        说明：Vault存款从合约账户扣款，不是现货账户
        """
        usdc = float(delta.get('usdc', 0))
        
        # 份额变化：输出为字符串公式
        share_change = f"-{usdc}/current_net_value"
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'vaultDeposit',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（无）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化：合约账户USDC减少
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': -usdc,
            
            # 份额变化（字符串公式）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {'usdc': usdc}
        }
    
    def record_vault_withdraw_impact(self, delta: Dict) -> Dict:
        """
        记录金库取款
        
        资产和份额变化：
        - perp_asset_change_ex_position: netWithdrawnUsd（合约账户USDC增加）
        - share_change: netWithdrawnUsd/current_net_value（份额增加）
        
        说明：Vault取款资金进入合约账户，不是现货账户
        """
        net_withdrawn = float(delta.get('netWithdrawnUsd', 0))
        
        # 份额变化：输出为字符串公式
        share_change = f"{net_withdrawn}/current_net_value"
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'vaultWithdraw',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（无）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化：合约账户USDC增加
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': net_withdrawn,
            
            # 份额变化（字符串公式）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {'netWithdrawnUsd': net_withdrawn}
        }

    def record_vault_distribution_impact(self, delta: Dict) -> Dict:
        """
        记录金库收益分配
        
        资产和份额变化：
        - perp_asset_change_ex_position: usdc（合约账户USDC增加）
        - share_change: usdc/current_net_value（增加份额）
        
        说明：Vault收益分配进入合约账户，不是现货账户
        """
        usdc = float(delta.get('usdc', 0))
        
        # 份额变化：输出为字符串公式（正值，表示增加）
        share_change = f"{usdc}/current_net_value"
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'vaultDistribution',
            
            # 持仓变化（无）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化：合约账户USDC增加
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': usdc,
            
            # 份额变化（字符串公式）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {
                'usdc': usdc,
                'vault': delta.get('vault', '')
            }
        }
    
    def record_vault_create_impact(self, delta: Dict) -> Dict:
        """
        记录创建金库
        
        资产和份额变化：
        - spot_position_changes: {'USDC': -(usdc + fee)}（减少现货USDC）
        - share_change: -(usdc + fee)/current_net_value（减少份额）
        - 不计入资产变化（完全体现在持仓变化中）
        
        说明：
        创建Vault需要：
        1. 存入初始资金(usdc)到Vault作为种子资金
        2. 支付创建费用(fee)给平台
        总共减少 (usdc + fee) 的USDC，相当于提现操作
        """
        usdc = float(delta.get('usdc', 0))      # 存入Vault的初始资金
        fee = float(delta.get('fee', 0))        # 创建费用
        total_cost = usdc + fee                  # 总成本
        
        # 创建Vault，现货USDC减少（包括存入金额和费用）
        spot_position_changes = {
            'USDC': {
                'change': -total_cost
            }
        }
        
        # 份额变化：输出为字符串公式（负值，表示减少）
        share_change = f"-{total_cost}/current_net_value"
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'vaultCreate',
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,  # 不计入资产变化
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（字符串公式）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {
                'usdc': usdc,
                'fee': fee,
                'vault': delta.get('vault', '')
            }
        }

    def record_spot_genesis_impact(self, delta: Dict, event_data: Dict = None) -> Dict:
        """
        记录现货空投（记录原操作的影响）
        
        持仓变化：
        - spot_position_changes: {'token': amount}
        
        份额变化（可选）：
        - 如果能获取到价格，则计算 share_change = (amount * price) / current_net_value
        - 如果无法获取价格（新币上线），则 share_change = 0
        
        参数:
            delta: 空投事件的 delta 字段
            event_data: 完整的事件数据（包含时间戳），用于获取价格
        """
        token = delta.get('token', '')
        amount = float(delta.get('amount', 0))
        
        # 空投增加现货持仓
        spot_position_changes = {
            token: {
                'change': amount  # 原操作：收到空投（正数）
            }
        }
        
        # 尝试获取价格并计算份额变化
        share_change = 0
        price = None
        
        if event_data and 'time' in event_data:
            # 尝试获取该时刻的现货价格
            import sys
            from pathlib import Path
            
            # 尝试导入 kline_fetcher（支持多种导入方式）
            try:
                from .kline_fetcher import get_open_price
            except ImportError:
                try:
                    from kline_fetcher import get_open_price
                except ImportError:
                    from main.kline_fetcher import get_open_price
            
            timestamp = event_data.get('time')
            try:
                # 使用 kline_fetcher 获取现货价格
                result = get_open_price(
                    coin=token,
                    coin_type='spot',
                    interval='auto',  # 自动选择最佳精度
                    timestamp=timestamp,
                    debug=False  # 默认不显示调试信息
                )
                
                if result and result.get('open'):
                    price = result['open']
                    # 计算USDC价值
                    usdc_value = amount * price
                    # 份额变化公式
                    share_change = f"{usdc_value}/current_net_value"
            except Exception as e:
                # 如果获取价格失败，份额变化为0
                print(f"  ⚠️  警告: 无法获取空投币种 {token} 的价格，份额变化设为0。错误: {e}")
                share_change = 0
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'spotGenesis',
            'coin': token,
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（根据是否能获取价格决定）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {
                'token': token,
                'amount': amount,
                'price': price if price is not None else 'unavailable'
            }
        }
    
    def record_send_impact(self, delta: Dict) -> Dict:
        """
        记录send事件（复杂转账）- 记录原操作的影响
        
        处理6种情况：
        1. 同账户 Perp → Spot（内部转账）
        2. 同账户 Spot → Perp（内部转账）
        3. Perp 转出到外部地址/DEX（包括转到 xyz 等外部DEX，份额减少）
        4. Spot 转出到外部地址/DEX（包括转到 xyz 等外部DEX，份额减少）
        5. 外部地址/DEX转入到 Perp（包括从 xyz 等外部DEX转入，份额增加）
        6. 外部地址/DEX转入到 Spot（包括从 xyz 等外部DEX转入，份额增加）
        """
        token = delta.get('token', 'USDC')
        amount = float(delta.get('amount', 0))
        usdcValue = float(delta.get('usdcValue', 0))
        user = delta.get('user', '').lower()
        destination = delta.get('destination', '').lower()
        sourceDex = delta.get('sourceDex', '')
        destinationDex = delta.get('destinationDex', '')
        fee = float(delta.get('fee', 0))
        fee_token = delta.get('feeToken', '')
        
        if not self.account_address:
            return self._empty_impact({'type': 'ledger', 'subtype': 'send'})
        
        # 初始化变量
        spot_position_changes = {}
        spot_asset_change_ex_position = 0
        perp_asset_change_ex_position = 0
        share_change = 0
        
        # 情况1: 同账户 Perp → Spot（内部转账）**肯定对我验证了
        if (user == self.account_address and destination == self.account_address and 
            sourceDex == '' and destinationDex == 'spot'):
            spot_position_changes = {
                'USDC': {'change': amount}  # 现货USDC增加
            }
            perp_asset_change_ex_position = -(fee + amount)  # 合约资产减少
            share_change = 0  # 内部转账不影响份额
        
        # 情况2: 同账户 Spot → Perp（内部转账）**肯定对我验证了
        elif (user == self.account_address and destination == self.account_address and 
              sourceDex == 'spot' and destinationDex == ''):
            spot_position_changes = {
                'USDC': {'change': -amount}  # 现货USDC减少
            }
            perp_asset_change_ex_position = amount  # 合约资产增加
            spot_asset_change_ex_position = -fee  # 现货手续费
            share_change = 0  # 内部转账不影响份额
        
        # 情况3: Perp 转出到外部地址或外部DEX
        # 包括：destination != account 或者 destination == account 但 destinationDex 不是空或spot
        elif (user == self.account_address and sourceDex == '' and 
              (destination != self.account_address or 
               (destination == self.account_address and destinationDex != '' and destinationDex != 'spot'))):
            # 检查token类型，只允许USDC
            if token != 'USDC':
                raise ValueError(
                    f"[错误] 情况3（Perp转出到外部地址/DEX）只支持USDC！\n"
                    f"  token: {token}\n"
                    f"  amount: {amount}\n"
                    f"  user: {user}\n"
                    f"  destination: {destination}\n"
                    f"  sourceDex: {sourceDex}\n"
                    f"  destinationDex: {destinationDex}"
                )
            perp_asset_change_ex_position = -(amount + fee)  # 合约资产减少
            # 份额减少：计算实际数值
            share_numerator = -(usdcValue + fee)
            share_change = f"{share_numerator}/current_net_value"
        
        # 情况4: Spot 转出到外部地址或外部DEX
        # 包括：destination != account 或者 destination == account 但 destinationDex 不是空或空字符串（Perp）
        elif (user == self.account_address and sourceDex == 'spot' and 
              (destination != self.account_address or 
               (destination == self.account_address and destinationDex != '' and destinationDex != 'spot'))):
            spot_position_changes = {
                token: {'change': -amount}  # 现货持仓减少
            }
            spot_asset_change_ex_position = -fee  # 现货手续费
            # 份额减少：计算实际数值
            share_numerator = -(usdcValue + fee)
            share_change = f"{share_numerator}/current_net_value"
        
        # 情况5: 外部地址或外部DEX转入到 Perp
        # 包括：user != account 或者 user == account 但 sourceDex 不是空或spot（从外部DEX转入）
        elif (destination == self.account_address and destinationDex == '' and 
              (user != self.account_address or 
               (user == self.account_address and sourceDex != '' and sourceDex != 'spot'))):
            # 检查token类型，只允许USDC
            if token != 'USDC':
                raise ValueError(
                    f"[错误] 情况5（外部地址/DEX转入到Perp）只支持USDC！\n"
                    f"  token: {token}\n"
                    f"  amount: {amount}\n"
                    f"  user: {user}\n"
                    f"  destination: {destination}\n"
                    f"  sourceDex: {sourceDex}\n"
                    f"  destinationDex: {destinationDex}"
                )
            perp_asset_change_ex_position = amount  # 合约资产增加
            share_change = f"{usdcValue}/current_net_value"  # 份额增加
        
        # 情况6: 外部地址或外部DEX转入到 Spot
        # 包括：user != account 或者 user == account 但 sourceDex 不是空或空字符串（Perp）
        elif (destination == self.account_address and destinationDex == 'spot' and 
              (user != self.account_address or 
               (user == self.account_address and sourceDex != '' and sourceDex != 'spot'))):
            spot_position_changes = {
                token: {'change': amount}  # 现货持仓增加
            }
            share_change = f"{usdcValue}/current_net_value"  # 份额增加
        
        # 情况7: 不匹配任何已知情况，报错
        else:
            raise ValueError(
                f"[错误] send事件不匹配任何已知情况！\n"
                f"  token: {token}\n"
                f"  amount: {amount}\n"
                f"  user: {user}\n"
                f"  destination: {destination}\n"
                f"  sourceDex: {sourceDex}\n"
                f"  destinationDex: {destinationDex}\n"
                f"  account_address: {self.account_address}"
            )
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'send',
            'coin': token,
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（除持仓影响）
            'spot_asset_change_ex_position': spot_asset_change_ex_position,
            'perp_asset_change_ex_position': perp_asset_change_ex_position,
            
            # 份额变化（字符串公式或0）
            'share_change': share_change,
            
            # 原始数据
            'raw_data': {
                'token': token,
                'amount': amount,
                'usdcValue': usdcValue,
                'fee': fee,
                'feeToken': fee_token,
                'user': user,
                'destination': destination,
                'sourceDex': sourceDex,
                'destinationDex': destinationDex,
            }
        }
    
    def record_staking_transfer_impact(self, delta: Dict) -> Dict:
        """
        记录质押转账（cStakingTransfer）- 对账户无影响
        
        质押转账只是将代币在可用状态和质押状态之间转移，
        不影响账户的总资产和份额，因此所有变化都为0。
        
        参数：
        - token: 质押的代币（如 HYPE）
        - amount: 质押数量
        - isDeposit: true表示存入质押，false表示取出质押
        """
        token = delta.get('token', '')
        amount = float(delta.get('amount', 0))
        is_deposit = delta.get('isDeposit', True)
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'cStakingTransfer',
            'coin': token,
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（质押不影响持仓）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化（质押不影响总资产）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（质押不影响份额）
            'share_change': 0,
            
            # 原始数据
            'raw_data': {
                'token': token,
                'amount': amount,
                'isDeposit': is_deposit,
            }
        }
    
    def record_liquidation_impact(self, delta: Dict) -> Dict:
        """
        记录清算事件（liquidation）- 不记录影响
        
        清算事件在 ledger 中只是概览记录，实际的交易细节会在 trades 中记录。
        因此 ledger 中的清算事件不需要记录影响，所有变化都为0。
        
        trades 中会有对应的清算交易记录，包含：
        - dir: "Liquidated Cross Long" 或 "Liquidated Cross Short"
        - liquidation.liquidatedUser: 被清算的用户地址
        - closedPnl: 实际的盈亏
        
        参数：
        - liquidatedNtlPos: 被清算的名义持仓规模
        - accountValue: 清算时账户价值
        - leverageType: 杠杆类型（Cross/Isolated）
        - liquidatedPositions: 被清算的持仓列表
        """
        liquidated_ntl_pos = delta.get('liquidatedNtlPos', '')
        account_value = delta.get('accountValue', '')
        leverage_type = delta.get('leverageType', '')
        liquidated_positions = delta.get('liquidatedPositions', [])
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'liquidation',
            'coin': '',
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（在 trades 中记录）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化（在 trades 中记录）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（在 trades 中记录）
            'share_change': 0,
            
            # 原始数据（保留供参考）
            'raw_data': {
                'liquidatedNtlPos': liquidated_ntl_pos,
                'accountValue': account_value,
                'leverageType': leverage_type,
                'liquidatedPositions': liquidated_positions,
            }
        }
    
    def record_rewards_claim_impact(self, delta: Dict, event_data: Dict = None) -> Dict:
        """
        记录交易奖励领取（rewardsClaim）
        
        处理逻辑：
        1. token 为空字符串或 'USDC' 时：
           - 增加现货账户（spot）的 USDC
           - 份额变化 = amount / current_net_value
        
        2. token 为其他代币时：
           - 增加现货账户该代币的持仓
           - 需要获取该代币的价格
           - 份额变化 = (price * amount) / current_net_value
        
        参数:
            delta: 奖励领取事件的 delta 字段
            event_data: 完整的事件数据（包含时间戳），用于获取价格
        """
        token = delta.get('token', '')
        amount = float(delta.get('amount', 0))
        
        # 判断是 USDC 还是其他代币
        if token == '' or token == 'USDC':
            # 情况1: USDC 奖励
            token = 'USDC'
            
            # 增加现货 USDC
            spot_position_changes = {
                'USDC': {
                    'change': amount
                }
            }
            
            # 份额变化：直接使用 amount
            share_change = f"{amount}/current_net_value"
            
            return {
                'event_type': 'ledger',
                'event_subtype': 'rewardsClaim',
                'coin': token,
                
                # 交易前持仓（非交易事件为空）
                'before_spot_trade': {},
                'before_perp_trade': {},
                
                # 持仓变化
                'spot_position_changes': spot_position_changes,
                'perp_position_changes': {},
                
                # 资产变化（除持仓影响）
                'spot_asset_change_ex_position': 0,
                'perp_asset_change_ex_position': 0,
                
                # 份额变化
                'share_change': share_change,
                
                # 原始数据
                'raw_data': {
                    'token': token,
                    'amount': amount,
                }
            }
        
        else:
            # 情况2: 其他代币奖励
            # 增加现货持仓
            spot_position_changes = {
                token: {
                    'change': amount
                }
            }
            
            # 尝试获取价格并计算份额变化
            share_change = 0
            price = None
            
            if event_data and 'time' in event_data:
                # 尝试导入 kline_fetcher（支持多种导入方式）
                try:
                    from .kline_fetcher import get_open_price
                except ImportError:
                    try:
                        from kline_fetcher import get_open_price
                    except ImportError:
                        from main.kline_fetcher import get_open_price
                
                timestamp = event_data.get('time')
                try:
                    # 使用 kline_fetcher 获取现货价格
                    result = get_open_price(
                        coin=token,
                        coin_type='spot',
                        interval='auto',  # 自动选择最佳精度
                        timestamp=timestamp,
                        debug=False  # 默认不显示调试信息
                    )
                    
                    if result and result.get('open'):
                        price = result['open']
                        # 计算USDC价值
                        usdc_value = amount * price
                        # 份额变化公式
                        share_change = f"{usdc_value}/current_net_value"
                except Exception as e:
                    # 如果获取价格失败，份额变化为0
                    print(f"  ⚠️  警告: 无法获取奖励币种 {token} 的价格，份额变化设为0。错误: {e}")
                    share_change = 0
            
            return {
                'event_type': 'ledger',
                'event_subtype': 'rewardsClaim',
                'coin': token,
                
                # 交易前持仓（非交易事件为空）
                'before_spot_trade': {},
                'before_perp_trade': {},
                
                # 持仓变化
                'spot_position_changes': spot_position_changes,
                'perp_position_changes': {},
                
                # 资产变化（除持仓影响）
                'spot_asset_change_ex_position': 0,
                'perp_asset_change_ex_position': 0,
                
                # 份额变化（根据是否能获取价格决定）
                'share_change': share_change,
                
                # 原始数据
                'raw_data': {
                    'token': token,
                    'amount': amount,
                    'price': price if price is not None else 'unavailable'
                }
            }
    
    def record_activate_dex_abstraction_impact(self, delta: Dict) -> Dict:
        """
        记录 DEX 抽象层激活（activateDexAbstraction）- 不记录影响
        
        这是一个授权/激活事件，类似于 ERC20 的 approve 操作。
        不产生实际的资金转移，只是记录用户在外部 DEX 上的授权状态。
        实际的资金转移会在 send 事件中记录。
        因此本事件不需要记录影响，所有变化都为0。
        
        参数：
        - dex: 外部 DEX 名称（如 "xyz"）
        - token: 代币类型（通常是 "USDC"）
        - amount: 授权额度或余额记录（不是实际转移金额）
        
        说明：
        - 通过多地址验证发现，amount 差异极大（从 0.0005 到 1,152,789）
        - 大部分记录前后没有配套的 send 事件
        - 因此判断这是授权机制，不是实际资金转移
        """
        dex = delta.get('dex', '')
        token = delta.get('token', '')
        amount = delta.get('amount', '')
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'activateDexAbstraction',
            'coin': token,
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化（授权不影响持仓）
            'spot_position_changes': {},
            'perp_position_changes': {},
            
            # 资产变化（授权不影响资产）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（授权不影响份额）
            'share_change': 0,
            
            # 原始数据（保留供参考）
            'raw_data': {
                'dex': dex,
                'token': token,
                'amount': amount,
            }
        }
    
    def record_account_activation_gas_impact(self, delta: Dict) -> Dict:
        """
        记录账户激活 Gas 费用（accountActivationGas）
        
        这是账户首次使用时支付的激活费用，用代币（通常是 HYPE）支付。
        
        处理逻辑：
        - 减少现货代币持仓: spot_position_changes = {token: -amount}
        - 不影响资产变化（已体现在持仓变化中）
        - 不影响份额（Gas 费用类似于手续费，不计入份额）
        
        参数：
        - token: 支付的代币类型（通常是 "HYPE" 或 "USDT0"）
        - amount: 支付的数量
        
        说明：
        - 通常发生在账户历史的最开始（首次激活）
        - 金额较小（0.018 ~ 1.0）
        - 类似于区块链上的 Gas 费用
        """
        token = delta.get('token', '')
        amount = float(delta.get('amount', 0))
        
        # 减少现货代币持仓
        spot_position_changes = {
            token: {
                'change': -amount  # 负数，表示支付
            }
        }
        
        return {
            'event_type': 'ledger',
            'event_subtype': 'accountActivationGas',
            'coin': token,
            
            # 交易前持仓（非交易事件为空）
            'before_spot_trade': {},
            'before_perp_trade': {},
            
            # 持仓变化
            'spot_position_changes': spot_position_changes,
            'perp_position_changes': {},
            
            # 资产变化（已体现在持仓变化中，这里为0）
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': 0,
            
            # 份额变化（Gas 费用不影响份额）
            'share_change': 0,
            
            # 原始数据
            'raw_data': {
                'token': token,
                'amount': amount,
            }
        }
    
    # ==================== 功能函数 ====================
    
    def export_to_csv(self, output_path: str):
        """
        将所有事件影响导出为CSV文件（简化版）
        
        Args:
            output_path: 输出CSV文件路径
        """
        import pandas as pd
        
        if not self.impacts:
            print("[警告] 没有影响记录，请先调用 process_all_events()")
            return
        
        # 准备CSV数据
        csv_data = []
        for impact in self.impacts:
            raw = impact.get('raw_data', {})
            
            row = {
                '事件序号': impact.get('event_number', ''),
                '时间': impact.get('event_time_str', ''),
                '时间戳': impact.get('event_time', ''),
                '事件大类': impact.get('event_type', ''),
                '事件类型': impact.get('event_subtype', ''),
                'oid': raw.get('oid', ''),
                'sz': self._format_number(raw.get('sz', '')),
                'fee': self._format_number(raw.get('fee', '')),
                'feeToken': raw.get('feeToken', ''),
                'closedPnl': self._format_number(raw.get('closedPnl', '')),
                'startPosition': self._format_number(raw.get('startPosition', '')),
                'usdc': self._format_number(raw.get('usdc', '')),
                'usdcValue': self._format_number(raw.get('usdcValue', '')),
                'toPerp': raw.get('toPerp', ''),
                'user': raw.get('user', ''),
                'destination': raw.get('destination', ''),
                '现货持仓变化': self._format_position_dict(impact.get('spot_position_changes', {})),
                '合约持仓变化': self._format_position_dict(impact.get('perp_position_changes', {})),
                '除持仓影响的现货资产变化': self._format_number(impact.get('spot_asset_change_ex_position', '')),
                '除持仓影响的合约资产变化': self._format_number(impact.get('perp_asset_change_ex_position', '')),
                '总份额变化': impact.get('share_change', ''),
            }
            
            csv_data.append(row)
        
        # 导出CSV（避免科学计数法）
        df = pd.DataFrame(csv_data)
        # 确保时间戳列为整数类型，避免科学计数法
        if '时间戳' in df.columns:
            df['时间戳'] = df['时间戳'].astype('int64')
        # 使用QUOTE_NONNUMERIC确保字符串字段被正确引用
        import csv
        df.to_csv(output_path, index=False, encoding='utf-8-sig', float_format='%.8f', quoting=csv.QUOTE_NONNUMERIC)
        
        print(f"[OK] 导出CSV成功: {output_path}")


    
    # ==================== 辅助方法 ====================
    
    def _format_before_trade(self, before_trade: Dict) -> str:
        """
        格式化交易前持仓为字符串（避免科学计数法）
        
        支持两种格式：
        1. 现货：{'coin': 'BTC', 'amount': 1.5}
        2. 合约：{'coin': 'BTC', 'amount': 410.85977, 'dir': 'long'}
        
        Args:
            before_trade: 交易前持仓字典
        
        Returns:
            格式化后的字符串
        """
        if not before_trade:
            return ''
        
        coin = before_trade.get('coin', '')
        amount = before_trade.get('amount', 0)
        dir_value = before_trade.get('dir', '')  # 合约独有
        
        if not coin:
            return ''
        
        # 格式化数量，避免科学计数法
        formatted_amount = f"{amount:.10f}".rstrip('0').rstrip('.')
        
        # 构建输出字符串
        if dir_value:
            # 合约格式：{'coin': 'BTC', 'amount': 410.85977, 'dir': 'long'}
            return f"{{'coin': '{coin}', 'amount': {formatted_amount}, 'dir': '{dir_value}'}}"
        else:
            # 现货格式：{'coin': 'BTC', 'amount': 1.5}
            return f"{{'coin': '{coin}', 'amount': {formatted_amount}}}"
    
    def _format_number(self, value) -> str:
        """
        格式化数值，避免科学计数法
        
        Args:
            value: 数值或字符串
        
        Returns:
            格式化后的字符串（无科学计数法）
        """
        if value == '' or value is None:
            return ''
        
        try:
            # 转换为浮点数
            num = float(value)
            if num == 0:
                return '0'
            
            # 使用:.10f格式化，然后去除末尾的0和小数点
            formatted = f"{num:.10f}".rstrip('0').rstrip('.')
            return formatted
        except (ValueError, TypeError):
            # 如果无法转换为数值，返回原值
            return str(value) if value else ''
    
    def _to_beijing_time(self, timestamp_ms: int) -> datetime:
        """
        将时间戳（毫秒）转换为北京时间
        
        Args:
            timestamp_ms: 毫秒时间戳
        
        Returns:
            北京时间的datetime对象
        """
        # 先转换为UTC时间（不受系统时区影响）
        utc_time = datetime.utcfromtimestamp(timestamp_ms / 1000)
        # 加8小时转换为北京时间（UTC+8）
        beijing_time = utc_time + timedelta(hours=8)
        return beijing_time
    
    def _to_beijing_time_str(self, timestamp_ms: int) -> str:
        """
        将时间戳（毫秒）转换为北京时间字符串（纯文本格式）
        
        Args:
            timestamp_ms: 毫秒时间戳
        
        Returns:
            格式化的北京时间字符串（只保留到秒）
        """
        beijing_time = self._to_beijing_time(timestamp_ms)
        time_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S')
        return time_str
    
    def _empty_impact(self, event: Dict) -> Dict:
        """返回空影响记录"""
        return {
            'event_type': event.get('type', 'unknown'),
            'event_subtype': event.get('subtype', ''),
            'before_spot_trade': {},
            'before_perp_trade': {},
            'spot_position_changes': {},
            'perp_position_changes': {},
            'spot_asset_change_ex_position': 0,
            'perp_asset_change_ex_position': 0,
            'share_change': 0,
            'raw_data': {}
        }
    
    def _format_position_dict(self, positions: Dict) -> str:
        """
        格式化持仓变化为字典字符串（避免科学计数法）
        
        支持三种格式：
        1. 合约交易新格式：包含 amount、price、dir 的字典
        2. 原操作格式：包含 change 的字典
        3. 数字值：直接的持仓变化量
        
        返回格式示例：
        - 合约交易：{'BTC': 'amount': 123, 'price': 10000, 'dir': Open Long}
        - 其他：{'BTC': 1000, 'ETH': -20}
        """
        if not positions:
            return ''
        
        result = {}
        for coin, info in positions.items():
            if isinstance(info, dict):
                # 检查是否为新的合约交易格式（包含 amount、price、dir）
                if 'amount' in info and 'price' in info and 'dir' in info:
                    # 格式化为：'amount': 123, 'price': 10000, 'dir': Open Long
                    amount = info['amount']
                    price = info['price']
                    dir_value = info['dir']
                    
                    # 格式化数字，避免科学计数法
                    formatted_amount = f"{amount:.10f}".rstrip('0').rstrip('.')
                    formatted_price = f"{price:.10f}".rstrip('0').rstrip('.')
                    
                    result[coin] = f"'amount': {formatted_amount}, 'price': {formatted_price}, 'dir': {dir_value}"
                # 检查是否为旧格式（包含 change）
                elif 'change' in info:
                    change = info.get('change', 0)
                    if change != 0:
                        result[coin] = change
            elif info != 0:
                result[coin] = info
        
        if not result:
            return ''
        
        # 手动格式化为字符串
        items = []
        for coin, value in result.items():
            if isinstance(value, str):
                # 新格式：已经格式化好的字符串
                items.append(f"'{coin}': {value}")
            elif isinstance(value, (int, float)):
                # 旧格式：数字
                formatted_value = f"{value:.10f}".rstrip('0').rstrip('.')
                items.append(f"'{coin}': {formatted_value}")
            else:
                items.append(f"'{coin}': {value}")
        
        return '{' + ', '.join(items) + '}'


# ==================== 主函数 ====================

def main():
    """
    主函数 - 完整使用示例
    
    使用方法：
        python event_impact_recorder.py <账户地址>
    """
    import sys
    
    if len(sys.argv) < 2:
        print("使用方法: python event_impact_recorder.py <账户地址>")
        sys.exit(1)
    
    address = sys.argv[1]
    
    # 创建记录器并处理
    recorder = EventImpactRecorder(address)
    recorder.process_all_events()
    
    # 导出CSV
    output_csv = f"{address}_impacts.csv"
    recorder.export_to_csv(output_csv)
    print(f"完成: {output_csv}")


if __name__ == "__main__":
    main()
