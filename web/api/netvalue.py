# -*- coding: utf-8 -*-
"""
净值数据 API
============

提供净值数据查询接口
"""
from flask import jsonify, request
from . import api_bp


@api_bp.route('/netvalue/<interval>/<address>')
def get_net_value(interval, address):
    """
    获取指定地址和时间区间的净值数据
    
    Args:
        interval: 时间区间（如 1h, 1d）
        address: 账户地址（完整地址）
        
    Query Params:
        from_first_trade: 是否从第一笔交易开始（默认 true）
    
    Returns:
        JSON响应，包含净值数据和统计信息
    """
    from web.app import db_manager
    
    # 解析 from_first_trade 参数（默认 true）
    from_first_trade_param = request.args.get('from_first_trade', 'true').strip().lower()
    from_first_trade = from_first_trade_param not in ['false', '0', 'no']
    
    try:
        # 获取第一笔交易时间戳
        first_trade_timestamp = db_manager.get_first_trade_timestamp(address.lower())
        
        # 获取数据
        df = db_manager.query_net_value_data(address, interval)
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': '没有找到数据'
            }), 404
        
        # 只保留有份额的数据（过滤掉初始的空数据）
        df = df[abs(df['total_shares']) > 1e-10].copy()
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': '没有找到有效数据（所有数据的份额都为0）'
            }), 404
        
        # 如果 from_first_trade=true 且有第一笔交易时间，则过滤数据
        if from_first_trade and first_trade_timestamp:
            df = df[df['timestamp'] >= first_trade_timestamp].copy()
            
            if df.empty:
                return jsonify({
                    'success': False,
                    'error': '过滤后没有数据（第一笔交易时间晚于所有数据）'
                }), 404
        
        # 转换为前端需要的格式
        data = {
            'timestamps': df['timestamp'].tolist(),
            'net_values': df['net_value'].tolist(),
            'cumulative_pnl': df['cumulative_pnl'].tolist(),
            'total_assets': df['total_assets'].tolist(),
            'spot_account_value': df['spot_account_value'].tolist(),
            'perp_account_value': df['perp_account_value'].tolist(),
            'realized_pnl': df['realized_pnl'].tolist(),
            'virtual_pnl': df['virtual_pnl'].tolist(),
            'total_shares': df['total_shares'].tolist(),
        }
        
        # 计算统计信息
        stats = {
            'first_net_value': float(df.iloc[0]['net_value']),
            'last_net_value': float(df.iloc[-1]['net_value']),
            'first_pnl': float(df.iloc[0]['cumulative_pnl']),
            'last_pnl': float(df.iloc[-1]['cumulative_pnl']),
            'total_records': len(df),
            'start_time': int(df.iloc[0]['timestamp']),
            'end_time': int(df.iloc[-1]['timestamp']),
            'first_trade_timestamp': first_trade_timestamp,
            'from_first_trade': from_first_trade,
        }
        
        # 计算收益率
        if abs(stats['first_net_value']) > 1e-10:
            stats['return_rate'] = ((stats['last_net_value'] - stats['first_net_value']) / 
                                   stats['first_net_value']) * 100
        else:
            # 如果第一个净值为0或接近0，无法计算收益率
            stats['return_rate'] = 0
            stats['warning'] = '第一个净值为0，无法计算收益率'
        
        return jsonify({
            'success': True,
            'data': data,
            'stats': stats
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

