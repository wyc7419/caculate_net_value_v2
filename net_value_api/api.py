# -*- coding: utf-8 -*-
"""
净值数据 API - 供第三方系统调用
===============================

接口列表：
1. GET /netvalue/intervals - 查询所有可用时间周期
2. GET /netvalue/data/<interval>/<address> - 查询指定地址和周期的净值数据
"""

import sys
import os

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Blueprint, jsonify, request

# 创建蓝图
net_value_api_bp = Blueprint('net_value_api', __name__, url_prefix='/netvalue')

# 可选导出的字段列表（timestamp 始终导出，不在此列表中）
EXPORTABLE_FIELDS = [
    'net_value', 'cumulative_pnl', 'total_assets', 'total_shares',
    'spot_account_value', 'perp_account_value', 'realized_pnl', 'virtual_pnl'
]


def get_db_manager():
    """获取数据库管理器实例"""
    from main.net_value_timescale_manager import NetValueTimescaleManager
    from config import TIMESCALE_CONFIG
    return NetValueTimescaleManager(**TIMESCALE_CONFIG)


# ============================================================
# 接口1：查询所有可用时间周期
# ============================================================

@net_value_api_bp.route('/intervals', methods=['GET'])
def get_available_intervals():
    """
    查询所有可用的时间周期
    
    请求：
        GET /netvalue/intervals
    
    响应：
        {
            "success": true,
            "data": {
                "intervals": ["1h", "2h", "4h", "8h", "12h", "1d"],
                "descriptions": {
                    "1h": "1小时",
                    "2h": "2小时",
                    ...
                }
            }
        }
    """
    from config.settings import SUPPORTED_INTERVALS
    
    # 时间周期描述映射
    interval_descriptions = {
        '1h': '1小时',
        '2h': '2小时',
        '4h': '4小时',
        '8h': '8小时',
        '12h': '12小时',
        '1d': '1天'
    }
    
    # 只返回配置中支持的周期的描述
    descriptions = {k: v for k, v in interval_descriptions.items() if k in SUPPORTED_INTERVALS}
    
    return jsonify({
        'success': True,
        'data': {
            'intervals': SUPPORTED_INTERVALS,
            'descriptions': descriptions
        }
    })


# ============================================================
# 接口2：查询指定地址和周期的净值数据
# ============================================================

@net_value_api_bp.route('/data/<interval>/<address>', methods=['GET'])
def get_net_value_data(interval, address):
    """
    查询指定地址在指定周期下的净值数据
    
    请求：
        GET /netvalue/data/<interval>/<address>?fields=<fields>&from_first_trade=<bool>
        
        路径参数：
            interval: 时间周期（1h, 2h, 4h, 8h, 12h, 1d）
            address: 账户地址（0x开头的以太坊地址）
        
        查询参数：
            fields: 要导出的字段，逗号分隔（可选，默认all）
                - all: 导出全部字段
                - 单个字段: net_value
                - 多个字段: net_value,cumulative_pnl,total_assets
                
                可用字段（timestamp 始终导出）：
                    net_value, cumulative_pnl, total_assets, total_shares,
                    spot_account_value, perp_account_value, realized_pnl,
                    virtual_pnl
            
            from_first_trade: 是否从第一笔交易开始（可选，默认true）
                - true: 只返回从第一笔交易所在时间区间开始的数据
                - false: 返回所有数据
            
            normalize: 是否归一化处理（可选，默认true，仅 from_first_trade=true 且未分页时有效）
                - true（默认）: 将 net_value 归一化（以第一条记录的净值为基准=1.0）
                - false: 返回原始净值
                - 注意：使用分页时自动禁用归一化
            
            page: 页码（可选，默认不分页）
                - 从 1 开始
                - 指定后启用分页模式
            
            page_size: 每页记录数（可选，默认1000，最大5000）
                - 仅当指定 page 时有效
    
    响应示例：
        {
            "success": true,
            "data": {
                "address": "0x...",
                "interval": "1h",
                "fields": ["net_value", "cumulative_pnl", ...],
                "first_trade_timestamp": 1704067200000,
                "records": [...],
                "stats": {...}
            }
        }
    
    响应（失败）：
        {
            "success": false,
            "error": "错误信息"
        }
    """
    # 验证时间周期
    from config.settings import SUPPORTED_INTERVALS
    if interval not in SUPPORTED_INTERVALS:
        return jsonify({
            'success': False,
            'error': f'无效的时间周期: {interval}，支持的周期: {SUPPORTED_INTERVALS}'
        }), 400
    
    # 验证地址格式
    if not address or not address.startswith('0x') or len(address) != 42:
        return jsonify({
            'success': False,
            'error': '无效的地址格式，地址应为 0x 开头的 42 位字符串'
        }), 400
    
    # 解析 fields 参数
    fields_param = request.args.get('fields', 'all').strip().lower()
    
    if fields_param == 'all':
        selected_fields = EXPORTABLE_FIELDS.copy()
    else:
        # 解析逗号分隔的字段
        selected_fields = [f.strip() for f in fields_param.split(',') if f.strip()]
        
        # 验证字段是否有效
        invalid_fields = [f for f in selected_fields if f not in EXPORTABLE_FIELDS]
        if invalid_fields:
            return jsonify({
                'success': False,
                'error': f'无效的字段: {invalid_fields}，可用字段: {EXPORTABLE_FIELDS}'
            }), 400
        
        if not selected_fields:
            return jsonify({
                'success': False,
                'error': '未指定有效字段'
            }), 400
    
    # 解析 from_first_trade 参数（默认 true）
    from_first_trade_param = request.args.get('from_first_trade', 'true').strip().lower()
    from_first_trade = from_first_trade_param not in ['false', '0', 'no']
    
    # 解析 normalize 参数（默认 true，仅 from_first_trade=true 时有效）
    normalize_param = request.args.get('normalize', 'true').strip().lower()
    normalize = normalize_param not in ['false', '0', 'no'] and from_first_trade
    
    # 解析分页参数
    page_param = request.args.get('page', '').strip()
    page_size_param = request.args.get('page_size', '1000').strip()
    
    # 判断是否启用分页（只有指定 page 参数时才启用）
    use_pagination = bool(page_param)
    page = 1
    page_size = 1000
    
    if use_pagination:
        try:
            page = int(page_param)
            if page < 1:
                return jsonify({
                    'success': False,
                    'error': 'page 必须 >= 1'
                }), 400
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'page 必须是整数'
            }), 400
        
        try:
            page_size = int(page_size_param)
            if page_size < 1 or page_size > 5000:
                return jsonify({
                    'success': False,
                    'error': 'page_size 必须在 1-5000 之间'
                }), 400
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'page_size 必须是整数'
            }), 400
        
        # 分页时自动禁用归一化
        if normalize:
            normalize = False
    
    try:
        # 获取数据库管理器
        db_manager = get_db_manager()
        
        # 获取第一笔交易时间戳
        first_trade_timestamp = db_manager.get_first_trade_timestamp(address.lower())
        
        # 查询数据
        df = db_manager.query_net_value_data(address.lower(), interval)
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': '未找到该地址的净值数据'
            }), 404
        
        # 如果 from_first_trade=true 且有第一笔交易时间，则过滤数据
        if from_first_trade and first_trade_timestamp:
            # 找到第一笔交易所在的时间区间
            # 数据按时间正序排列，找到第一个 timestamp >= first_trade_timestamp 的区间
            df = df[df['timestamp'] >= first_trade_timestamp].copy()
            
            if df.empty:
                return jsonify({
                    'success': False,
                    'error': '过滤后没有数据（第一笔交易时间晚于所有数据）'
                }), 404
        
        # 记录总数（分页前）
        total_records = len(df)
        total_pages = 1
        
        # 分页处理
        if use_pagination:
            total_pages = (total_records + page_size - 1) // page_size  # 向上取整
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            if start_idx >= total_records:
                return jsonify({
                    'success': False,
                    'error': f'页码超出范围，总页数: {total_pages}'
                }), 400
            
            df = df.iloc[start_idx:end_idx]
        
        # 转换为记录列表（timestamp 始终导出，其他字段按选择导出）
        records = []
        for _, row in df.iterrows():
            timestamp = int(row['timestamp'])
            
            # 构建可选字段记录
            optional_fields = {
                'net_value': float(row['net_value']),
                'cumulative_pnl': float(row['cumulative_pnl']),
                'total_assets': float(row['total_assets']),
                'total_shares': float(row['total_shares']),
                'spot_account_value': float(row['spot_account_value']),
                'perp_account_value': float(row['perp_account_value']),
                'realized_pnl': float(row['realized_pnl']),
                'virtual_pnl': float(row['virtual_pnl'])
            }
            
            # timestamp 始终导出，其他字段按选择导出
            filtered_record = {'timestamp': timestamp}
            filtered_record.update({k: v for k, v in optional_fields.items() if k in selected_fields})
            records.append(filtered_record)
        
        # 归一化处理（仅当 normalize=true 且 net_value 在导出字段中）
        base_net_value = None
        if normalize and 'net_value' in selected_fields and len(records) > 0:
            base_net_value = records[0].get('net_value', 0)
            if abs(base_net_value) > 1e-10:
                for record in records:
                    if 'net_value' in record:
                        record['net_value'] = record['net_value'] / base_net_value
            else:
                # 基准净值为0，无法归一化
                base_net_value = None
        
        # 计算统计信息
        first_record = records[0]
        last_record = records[-1]
        
        # 计算收益率（归一化后直接用 last - first）
        if normalize and base_net_value:
            return_rate = (last_record['net_value'] - 1.0) * 100  # 归一化后起始值=1
        elif abs(first_record.get('net_value', 0)) > 1e-10:
            return_rate = ((last_record['net_value'] - first_record['net_value']) / 
                          first_record['net_value']) * 100
        else:
            return_rate = 0
        
        stats = {
            'total_records': len(records),
            'start_time': first_record['timestamp'],
            'end_time': last_record['timestamp'],
            'first_net_value': first_record.get('net_value', None),
            'last_net_value': last_record.get('net_value', None),
            'return_rate': round(return_rate, 4),
            'first_pnl': first_record.get('cumulative_pnl', None),
            'last_pnl': last_record.get('cumulative_pnl', None)
        }
        
        # 如果归一化了，添加原始基准净值
        if normalize and base_net_value:
            stats['base_net_value'] = base_net_value
        
        # 构建响应
        response_data = {
            'address': address.lower(),
            'interval': interval,
            'fields': selected_fields,
            'first_trade_timestamp': first_trade_timestamp,
            'from_first_trade': from_first_trade,
            'normalize': normalize,
            'records': records,
            'stats': stats
        }
        
        # 添加分页信息
        if use_pagination:
            response_data['pagination'] = {
                'page': page,
                'page_size': page_size,
                'total_records': total_records,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        
        return jsonify({
            'success': True,
            'data': response_data
        })
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'查询失败: {str(e)}'
        }), 500



