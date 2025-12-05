# -*- coding: utf-8 -*-
"""
数据加载器 - 统一的数据访问层
====================================

提供统一的数据访问接口，方便各个模块使用
数据来源：
1. 交易数据（trades）- 从 API 获取
2. 资金费数据（funding）- 从 API 获取
3. 账本数据（ledger）- 从 API 获取
4. 快照数据（snapshots）- 从 API 获取

使用示例：
---------
from data_loader import DataLoader

# 创建数据加载器（API 配置从 config 模块自动加载）
loader = DataLoader()

# 加载交易数据（从API）
trades = loader.load_trades(address='0x...')

# 加载指定时间范围的交易数据
trades = loader.load_trades(
    address='0x...', 
    start_time='2025-09-01T00:00:00Z',
    end_time='2025-10-01T00:00:00Z',
    range_type='Select'
)

配置说明：
---------
API 配置从 config.data_source 模块自动加载，可通过以下方式修改：
1. 修改 config/api.py 文件
2. 设置环境变量（如 API_BASE_URL, API_TIMEOUT）
3. 创建 .env 文件
"""

import json
import os
import sys
import requests
from typing import List, Dict, Optional
from datetime import datetime

# 添加父目录到路径，以便导入 config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_source import (
    API_BASE_URL,
    API_TIMEOUT,
    API_MAX_RETRIES,
    API_RETRY_DELAY,
    TRADES_API_ENDPOINT,
    FUNDING_API_ENDPOINT,
    LEDGER_API_ENDPOINT,
    DATA_SOURCE_TYPE
)


class DataLoader:
    """数据加载器 - 提供统一的数据访问接口（所有数据从 API 获取）"""
    
    # 类级别的现货代币映射缓存（避免重复加载）
    _spot_token_mapping_cache = None
    
    def __init__(self):
        """
        初始化数据加载器
        
        注意:
            - API 配置从 config.data_source 模块自动加载
            - 可通过环境变量修改配置（如 API_BASE_URL, API_TIMEOUT）
        """
        # 从配置加载 API 设置
        self.api_base_url = API_BASE_URL
        self.api_timeout = API_TIMEOUT
        self.api_max_retries = API_MAX_RETRIES
        self.api_retry_delay = API_RETRY_DELAY
        self.data_source_type = DATA_SOURCE_TYPE
        
        # 初始化现货代币映射缓存（只加载一次）
        self._init_spot_token_mapping()
    
    def _init_spot_token_mapping(self):
        """初始化现货代币映射缓存（@编号 -> 可读名称）"""
        if DataLoader._spot_token_mapping_cache is None:
            try:
                from .kline_fetcher import get_spot_token_mapping
                DataLoader._spot_token_mapping_cache = get_spot_token_mapping()
            except ImportError:
                try:
                    from kline_fetcher import get_spot_token_mapping
                    DataLoader._spot_token_mapping_cache = get_spot_token_mapping()
                except ImportError:
                    try:
                        from main.kline_fetcher import get_spot_token_mapping
                        DataLoader._spot_token_mapping_cache = get_spot_token_mapping()
                    except Exception as e:
                        print(f"[WARN] 无法加载现货代币映射: {e}")
                        DataLoader._spot_token_mapping_cache = {}
            except Exception as e:
                print(f"[WARN] 无法加载现货代币映射: {e}")
                DataLoader._spot_token_mapping_cache = {}
    
    def _resolve_spot_token_id(self, coin: str) -> str:
        """将 @开头的代币编号转换为可读名称
        
        Args:
            coin: 币种代码（如 @10, @1, BTC）
            
        Returns:
            转换后的名称（如 HYPE, PURR），如果不是 @ 开头或找不到则返回原值
        """
        if not coin or not coin.startswith('@'):
            return coin
        
        # 从缓存中查找
        if DataLoader._spot_token_mapping_cache:
            return DataLoader._spot_token_mapping_cache.get(coin, coin)
        
        return coin
        
    
    def _normalize_trade_fields(self, trade: Dict) -> Dict:
        """
        规范化交易数据字段
        
        将 API 的下划线命名转换为驼峰命名（兼容旧代码）
        转换时间格式
        添加缺失字段（包括推断 type 字段）
        """
        normalized = {}
        
        # 字段名称映射表（API字段 -> 标准字段）
        field_mapping = {
            'closed_pnl': 'closedPnl',
            'fee_token': 'feeToken',
            'start_position': 'startPosition',
            'builder_fee': 'builderFee',
            'twap_id': 'twapId',
            # 保持不变的字段
            'time': 'time',
            'address': 'address',
            'coin': 'coin',
            'side': 'side',
            'dir': 'dir',
            'px': 'px',
            'sz': 'sz',
            'fee': 'fee',
            'hash': 'hash',
            'crossed': 'crossed',
            'oid': 'oid',
            'tid': 'tid',
            'builder': 'builder',
            'cloid': 'cloid',
        }
        
        # 转换字段名
        for api_field, std_field in field_mapping.items():
            if api_field in trade:
                normalized[std_field] = trade[api_field]
        
        # 复制未映射的字段
        for key, value in trade.items():
            if key not in field_mapping and key not in normalized:
                normalized[key] = value
        
        # 时间格式转换（如果是字符串格式，转换为毫秒时间戳）
        if 'time' in normalized:
            time_value = normalized['time']
            if isinstance(time_value, str):
                try:
                    # 将 ISO 格式转换为时间戳（毫秒）
                    from datetime import datetime, timezone
                    import calendar
                    # 解析格式如: "2025-08-18 04:17:09.465000+0000"
                    # 去掉时区信息并解析
                    time_str = time_value.replace('+0000', '').strip()
                    dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
                    # 使用 calendar.timegm 确保视为 UTC 时间
                    normalized['time'] = int(calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond / 1000)
                except:
                    # 如果转换失败，保持原值
                    pass
        
        # 转换 @开头的币种为可读名称
        if 'coin' in normalized:
            coin = normalized['coin']
            if coin and coin.startswith('@'):
                normalized['coin'] = self._resolve_spot_token_id(coin)
        
        # 推断 type 字段（如果 API 未返回）
        if 'type' not in normalized:
            # 根据 dir 字段推断交易类型（基于实际业务规则）
            dir_value = normalized.get('dir', '')
            
            # 合约交易（perp）的完整 dir 值列表
            perp_dir_values = [
                'Close Long',
                'Close Short',
                'Open Long',
                'Open Short',
                'Auto-Deleveraging',
                'Short > Long',
                'Long > Short',
                'Settlement',
                'Liquidated Cross Short',
                'Liquidated Cross Long',
                'Liquidated Isolated Short',
                'Liquidated Isolated Long',
            ]
            
            # 现货交易（spot）的完整 dir 值列表
            spot_dir_values = [
                'Buy',
                'Sell',
                'Spot Dust Conversion',
            ]
            
            # 严格匹配判断
            if dir_value in perp_dir_values:
                # 已知的合约交易类型
                normalized['type'] = 'perp'
            elif dir_value in spot_dir_values:
                # 已知的现货交易类型
                normalized['type'] = 'spot'
            else:
                # dir 为空或不在预定义列表中，打印警告并默认为 perp
                coin = normalized.get('coin', 'N/A')
                time_val = normalized.get('time', 'N/A')
                
                if not dir_value or dir_value == '':
                    print(f"⚠️ [WARN] dir 字段为空，默认设为 perp")
                    print(f"    币种: {coin}, 时间: {time_val}")
                else:
                    print(f"⚠️ [WARN] 未知的 dir 值 '{dir_value}'，默认设为 perp")
                    print(f"    币种: {coin}, 时间: {time_val}")
                
                # 默认设置为 perp
                normalized['type'] = 'perp'
        
        return normalized
    
    def _normalize_funding_fields(self, funding: Dict) -> Dict:
        """
        规范化资金费数据字段
        
        将 API 的扁平结构转换为嵌套的 delta 结构（兼容旧代码）
        转换时间格式
        转换字段命名
        """
        normalized = {}
        
        # 时间格式转换（ISO字符串 → 毫秒时间戳）
        time_value = funding.get('time')
        if isinstance(time_value, str):
            try:
                # 解析 ISO 格式: "2025-07-23T00:00:00Z"
                from datetime import datetime, timezone
                import calendar
                # 移除 'Z' 并解析为 UTC 时间
                if time_value.endswith('Z'):
                    time_str = time_value[:-1]  # 移除 'Z'
                    dt = datetime.fromisoformat(time_str)
                else:
                    dt = datetime.fromisoformat(time_value)
                # 使用 calendar.timegm 确保视为 UTC 时间
                normalized['time'] = int(calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond / 1000)
            except Exception as e:
                # 如果转换失败，尝试其他方法或报错
                print(f"[WARN] 时间格式转换失败: {time_value}, 错误: {e}")
                # 默认设置为0
                normalized['time'] = 0
        elif isinstance(time_value, (int, float)):
            normalized['time'] = int(time_value)
        else:
            normalized['time'] = 0
        
        # coin 字段直接复制
        normalized['coin'] = funding.get('coin', '')
        
        # 构建 delta 嵌套字典（字符串格式）
        normalized['delta'] = {
            'usdc': str(funding.get('usdc', 0)),
            'szi': str(funding.get('szi', 0)),
            'fundingRate': str(funding.get('funding_rate', 0))
        }
        
        return normalized
    
    def _normalize_ledger_fields(self, ledger: Dict) -> Dict:
        """
        规范化账本数据字段
        
        只保留必要的3个字段（与数据库格式一致）
        转换时间格式
        """
        normalized = {}
        
        # 时间格式转换（ISO字符串 → 毫秒时间戳）
        time_value = ledger.get('time')
        if isinstance(time_value, str):
            try:
                # 解析 ISO 格式: "2025-05-12T05:05:40.815000Z"
                from datetime import datetime, timezone
                import calendar
                # 移除 'Z' 并解析为 UTC 时间
                if time_value.endswith('Z'):
                    time_str = time_value[:-1]  # 移除 'Z'
                    dt = datetime.fromisoformat(time_str)
                else:
                    dt = datetime.fromisoformat(time_value)
                # 使用 calendar.timegm 确保视为 UTC 时间
                normalized['time'] = int(calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond / 1000)
            except Exception as e:
                # 如果转换失败，尝试其他方法或报错
                print(f"[WARN] Ledger 时间格式转换失败: {time_value}, 错误: {e}")
                # 默认设置为0
                normalized['time'] = 0
        elif isinstance(time_value, (int, float)):
            normalized['time'] = int(time_value)
        else:
            normalized['time'] = 0
        
        # hash 字段直接复制
        normalized['hash'] = ledger.get('hash', '')
        
        # delta 字段直接使用（API已经提供完整的字典格式）
        delta = ledger.get('delta')
        if isinstance(delta, dict):
            normalized['delta'] = delta
        else:
            # 如果 delta 不是字典，创建一个空字典
            normalized['delta'] = {}
        
        return normalized
    
    # ==================== 交易数据（从 API 获取）====================
    
    def load_trades(
        self, 
        address: str, 
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        range_type: str = "All",
        page_size: int = 100000
    ) -> List[Dict]:
        """
        从 API 加载交易数据（支持分页，自动获取所有数据）
        
        参数:
            address: 账户地址
            start_time: 开始时间（ISO 8601 格式）
            end_time: 结束时间（ISO 8601 格式）
            range_type: 查询范围类型 "All" 或 "Select"
            page_size: 每页记录数（默认100000）
        
        返回:
            List[Dict]: 交易数据列表
        """
        all_trades = []
        page = 1
        
        try:
            api_url = TRADES_API_ENDPOINT
            
            while True:
                # 构建请求体
                payload = {
                    "address": address,
                    "range": range_type,
                    "page": page,
                    "page_size": page_size
                }
                
                # 如果是 Select 模式，添加时间范围
                if range_type == "Select":
                    if not start_time or not end_time:
                        print(f"[ERROR] Select 模式需要指定 start_time 和 end_time")
                        return []
                    payload["start_time"] = start_time
                    payload["end_time"] = end_time
                
                # 发送 POST 请求
                response = requests.post(
                    api_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=self.api_timeout
                )
                
                # 检查响应状态
                if response.status_code != 200:
                    error_msg = f"API 请求失败: HTTP {response.status_code}"
                    print(f"[ERROR] {error_msg}")
                    try:
                        error_detail = response.text[:500]
                        print(f"[ERROR] 响应内容: {error_detail}")
                    except:
                        pass
                    raise RuntimeError(error_msg)
                
                # 解析响应
                result = response.json()
                
                # 提取交易数据
                trades_data = []
                
                if isinstance(result, dict) and 'data' in result:
                    data_section = result['data']
                    if isinstance(data_section, dict) and 'data' in data_section:
                        raw_data = data_section['data']
                        columns = data_section.get('columns', [])
                        
                        if raw_data and columns:
                            for row in raw_data:
                                if isinstance(row, list) and len(row) == len(columns):
                                    trade_dict = dict(zip(columns, row))
                                    trade_dict = self._normalize_trade_fields(trade_dict)
                                    trades_data.append(trade_dict)
                                elif isinstance(row, dict):
                                    trade_dict = self._normalize_trade_fields(row)
                                    trades_data.append(trade_dict)
                        elif raw_data:
                            trades_data = [self._normalize_trade_fields(item) for item in raw_data]
                
                # 添加到总列表
                all_trades.extend(trades_data)
                
                # 检查是否还有更多数据
                if len(trades_data) < page_size:
                    break
                
                # 继续下一页
                page += 1
            
            # 循环结束后返回结果
            return all_trades
            
        except requests.exceptions.Timeout:
            raise RuntimeError("API 请求超时（加载交易数据）")
        except requests.exceptions.ConnectionError:
            raise RuntimeError("API 连接失败（加载交易数据）")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API 请求异常（加载交易数据）: {e}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"API 响应 JSON 解析失败（加载交易数据）: {e}")
        except RuntimeError:
            raise  # 重新抛出 RuntimeError
        except Exception as e:
            raise RuntimeError(f"加载交易数据失败: {e}")
    
    # ==================== 资金费数据（从 API 获取）====================
    
    def load_funding(self, address: str) -> List[Dict]:
        """
        从 API 加载资金费数据
        
        参数:
            address: 账户地址
        
        返回:
            List[Dict]: 资金费数据列表，按时间倒序排列
            格式: [{'time': timestamp, 'coin': str, 'delta': {'usdc': str, 'szi': str, 'fundingRate': str}}]
        """
        try:
            # 使用配置文件中的 API 端点
            api_url = FUNDING_API_ENDPOINT
            
            # 构建请求体
            payload = {
                "address": address,
                "range": "all"
            }
            
            # 发送 POST 请求
            response = requests.post(
                api_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.api_timeout
            )
            
            # 检查响应状态
            if response.status_code != 200:
                error_msg = f"API 请求失败: HTTP {response.status_code}"
                print(f"[ERROR] {error_msg}")
                try:
                    print(f"[ERROR] 响应内容: {response.text[:500]}")
                except:
                    pass
                raise RuntimeError(error_msg)
            
            # 解析响应
            result = response.json()
            
            # 提取资金费数据
            # API 响应格式: {'success': bool, 'message': str, 'metadata': {...}, 'records': [...]}
            funding_data = []
            
            if isinstance(result, dict) and 'records' in result:
                records = result['records']
                
                # 规范化每条记录
                for record in records:
                    if isinstance(record, dict):
                        normalized = self._normalize_funding_fields(record)
                        funding_data.append(normalized)
            
            # 按时间倒序排列（与原数据库查询保持一致）
            funding_data.sort(key=lambda x: x['time'], reverse=True)
            
            return funding_data
            
        except requests.exceptions.Timeout:
            raise RuntimeError("API 请求超时")
        except requests.exceptions.ConnectionError:
            raise RuntimeError("API 连接失败")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API 请求异常: {e}")
        except json.JSONDecodeError as e:
            print(f"[ERROR] API 响应 JSON 解析失败: {e}")
            try:
                print(f"[ERROR] 响应内容: {response.text[:500]}")
            except:
                pass
            raise RuntimeError(f"API 响应 JSON 解析失败: {e}")
            return []
        except Exception as e:
            print(f"[ERROR] 加载资金费数据失败: {e}")
            return []
    
    # ==================== 账本数据（从 API 获取）====================
    
    def load_ledger(self, address: str) -> List[Dict]:
        """
        从 API 加载账本数据（除资金费外的所有账本记录）
        
        参数:
            address: 账户地址
        
        返回:
            List[Dict]: 账本数据列表，按时间倒序排列
            格式: [{'time': timestamp, 'hash': str, 'delta': dict}]
        """
        try:
            # 使用配置文件中的 API 端点
            api_url = LEDGER_API_ENDPOINT
            
            # 构建请求体
            payload = {
                "address": address,
                "range": "all",
                "include_funding": False,  # 不包含资金费
                "limit": 100000
            }
            
            # 发送 POST 请求
            response = requests.post(
                api_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.api_timeout
            )
            
            # 检查响应状态
            if response.status_code != 200:
                error_msg = f"API 请求失败: HTTP {response.status_code}"
                print(f"[ERROR] {error_msg}")
                try:
                    print(f"[ERROR] 响应内容: {response.text[:500]}")
                except:
                    pass
                raise RuntimeError(error_msg)
            
            # 解析响应
            result = response.json()
            
            # 提取账本数据
            # API 响应格式: {'success': bool, 'message': str, 'metadata': {...}, 
            #               'ledger_records': [...], 'funding_records': [...]}
            ledger_data = []
            
            if isinstance(result, dict) and 'ledger_records' in result:
                records = result['ledger_records']
                
                # 规范化每条记录
                for record in records:
                    if isinstance(record, dict):
                        normalized = self._normalize_ledger_fields(record)
                        ledger_data.append(normalized)
            
            # 按时间倒序排列（与原数据库查询保持一致）
            ledger_data.sort(key=lambda x: x['time'], reverse=True)
            
            return ledger_data
            
        except requests.exceptions.Timeout:
            raise RuntimeError("API 请求超时（加载账本数据）")
        except requests.exceptions.ConnectionError:
            raise RuntimeError("API 连接失败（加载账本数据）")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API 请求异常（加载账本数据）: {e}")
        except json.JSONDecodeError as e:
            print(f"[ERROR] API 响应 JSON 解析失败: {e}")
            try:
                print(f"[ERROR] 响应内容: {response.text[:500]}")
            except:
                pass
            raise RuntimeError(f"API 响应 JSON 解析失败（加载账本数据）: {e}")
        except RuntimeError:
            raise  # 重新抛出 RuntimeError
        except Exception as e:
            raise RuntimeError(f"加载账本数据失败: {e}")
            return []
    
    # ==================== 快照数据（从 API 获取）====================
    
    def load_snapshots_from_api(
        self, 
        address: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        range_type: str = "All",
        page_size: int = 100
    ) -> Dict:
        """
        从 API 加载账户快照数据（支持分页，自动获取所有数据）
        
        参数:
            address: 账户地址
            start_time: 开始时间（ISO格式，可选）
            end_time: 结束时间（ISO格式，可选）
            range_type: 时间范围类型（"All" 或 "Select"）
            page_size: 每页记录数（默认100）
        
        返回:
            Dict: 包含三个部分的快照数据
        """
        # 累积所有页的数据
        all_account_summary = []
        all_positions = []
        all_spot_balances = []
        metadata = {}
        page = 1
        
        try:
            # 使用配置文件中的 API 端点
            from config.data_source import API_BASE_URL, API_TIMEOUT
            api_url = f"{API_BASE_URL}/api/v1/snapshots/query"
            
            while True:
                # 构建请求体
                payload = {
                    "address": address,
                    "include_details": True,
                    "range": range_type,
                    "page": page,
                    "page_size": page_size
                }
                
                if range_type == "Select" and start_time and end_time:
                    payload["start_time"] = start_time
                    payload["end_time"] = end_time
                
                # 发送 POST 请求
                response = requests.post(
                    api_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=API_TIMEOUT
                )
                
                # 检查响应状态
                if response.status_code != 200:
                    error_msg = f"API 请求失败: HTTP {response.status_code}"
                    print(f"[ERROR] {error_msg}")
                    try:
                        print(f"[ERROR] 响应内容: {response.text[:500]}")
                    except:
                        pass
                    raise RuntimeError(error_msg)
                
                # 解析响应
                result = response.json()
                
                if 'data' not in result:
                    error_msg = "API 响应格式异常: 缺少 data 字段"
                    print(f"[ERROR] {error_msg}")
                    raise RuntimeError(error_msg)
                
                data = result['data']
                metadata = result.get('metadata', {})
                
                # 转换数据格式（从 columns + data 转换为字典列表）
                account_summary = self._convert_snapshot_data(data.get('account_summary', {}))
                positions = self._convert_snapshot_data(data.get('positions', {}))
                spot_balances = self._convert_snapshot_data(data.get('spot_balances', {}))
                
                # 累积数据
                all_account_summary.extend(account_summary)
                all_positions.extend(positions)
                all_spot_balances.extend(spot_balances)
                
                # 检查是否还有更多数据
                if len(account_summary) < page_size:
                    break
                
                # 继续下一页
                page += 1
            
            # 循环结束后返回结果
            return {
                'account_summary': all_account_summary,
                'positions': all_positions,
                'spot_balances': all_spot_balances,
                'metadata': metadata
            }
            
        except requests.exceptions.Timeout:
            raise RuntimeError("API 请求超时（加载快照数据）")
        except requests.exceptions.ConnectionError:
            raise RuntimeError("API 连接失败（加载快照数据）")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API 请求异常（加载快照数据）: {e}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"API 响应 JSON 解析失败（加载快照数据）: {e}")
        except RuntimeError:
            raise  # 重新抛出 RuntimeError
        except Exception as e:
            raise RuntimeError(f"加载快照数据失败: {e}")
    
    def _convert_snapshot_data(self, snapshot_dict: Dict) -> List[Dict]:
        """
        将 columns + data 格式转换为字典列表
        
        参数:
            snapshot_dict: 包含 'columns' 和 'data' 的字典
        
        返回:
            List[Dict]: 转换后的字典列表
        """
        if not snapshot_dict or 'columns' not in snapshot_dict or 'data' not in snapshot_dict:
            return []
        
        columns = snapshot_dict['columns']
        data_rows = snapshot_dict['data']
        
        result = []
        for row in data_rows:
            row_dict = dict(zip(columns, row))
            result.append(row_dict)
        
        return result
    
    def _empty_snapshots_result(self) -> Dict:
        """返回空的快照数据结构"""
        return {
            'account_summary': [],
            'positions': [],
            'spot_balances': [],
            'metadata': {}
        }
    
    # ==================== 综合加载方法 ====================
    
    def load_all_events(self, address: str) -> Dict:
        """
        加载所有事件数据（交易、资金费、账本）
        
        参数:
            address: 账户地址
        
        返回:
            Dict: 包含所有事件数据的字典，格式兼容 EventImpactRecorder
                {
                    'address': address,
                    'data': {
                        'trade': [...],
                        'funding': [...],
                        'ledger': {'data': [...]}
                    }
                }
        """
        trades = self.load_trades(address)
        funding = self.load_funding(address)
        ledger = self.load_ledger(address)
        
        return {
            'address': address.lower(),
            'data': {
                'trade': trades,
                'funding': funding,
                'ledger': {
                    'data': ledger
                }
            }
        }


# ==================== 使用示例 ====================

def main():
    """使用示例"""
    import sys
    
    if len(sys.argv) < 2:
        print("使用方法: python data_loader.py <账户地址>")
        print("示例: python data_loader.py 0x1234567890abcdef")
        print("")
        print("注意: API 配置从 config/api.py 自动加载")
        print("     可通过环境变量修改，如: API_BASE_URL=http://...")
        sys.exit(1)
    
    address = sys.argv[1]
    
    print("="*80)
    print("数据加载器 - 测试（从API获取数据）")
    print("="*80)
    
    # 创建数据加载器（API 配置自动从 config 加载）
    loader = DataLoader()
    
    # 测试1: 加载所有交易数据
    print("\n测试1: 加载所有交易数据")
    print("-"*80)
    trades = loader.load_trades(address)
    print(f"加载到 {len(trades)} 条交易")
    if trades:
        print(f"第一条交易: {trades[0]}")
    
    # 测试2: 加载指定时间范围的交易数据
    print("\n测试2: 加载2025年9月的交易数据")
    print("-"*80)
    trades_sept = loader.load_trades(
        address=address,
        start_time="2025-09-01T00:00:00Z",
        end_time="2025-10-01T00:00:00Z",
        range_type="Select"
    )
    print(f"9月交易数: {len(trades_sept)}")
    
    # 测试3: 加载所有事件数据
    print("\n测试3: 加载所有事件数据")
    print("-"*80)
    events = loader.load_all_events(address)
    
    # 测试4: 从API加载快照数据
    print("\n测试4: 从API加载快照数据")
    print("-"*80)
    snapshots = loader.load_snapshots_from_api(address)
    print(f"账户摘要: {len(snapshots['account_summary'])} 条")
    print(f"持仓快照: {len(snapshots['positions'])} 条")
    print(f"现货余额: {len(snapshots['spot_balances'])} 条")
    
    print("\n" + "="*80)
    print("测试完成！")
    print("="*80)


if __name__ == "__main__":
    main()

