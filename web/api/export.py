# -*- coding: utf-8 -*-
"""
数据导出 API
============

提供CSV导出接口
"""
from flask import Response, jsonify
from datetime import datetime, timezone, timedelta
from io import StringIO
import pandas as pd
from . import api_bp


@api_bp.route('/export/<interval>/<address>')
def export_csv(interval, address):
    """
    导出指定地址和时间区间的净值数据为CSV
    
    Args:
        interval: 时间区间（如 1h, 1d）
        address: 账户地址（完整地址）
    
    Returns:
        CSV文件下载
    """
    from web.app import db_manager
    
    try:
        # 获取数据
        df = db_manager.query_net_value_data(address, interval)
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': '没有找到数据'
            }), 404
        
        # 准备导出数据
        export_df = df[[
            'timestamp',
            'spot_account_value',
            'realized_pnl',
            'virtual_pnl',
            'perp_account_value',
            'total_assets',
            'total_shares',
            'net_value',
            'cumulative_pnl'
        ]].copy()
        
        # 添加UTC+8时间列（在timestamp列后面）
        # 将毫秒时间戳转换为UTC+8时间
        utc8_times = []
        for ts in export_df['timestamp']:
            # 毫秒时间戳转为秒
            dt_utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            # 转换为UTC+8
            dt_utc8 = dt_utc.astimezone(timezone(timedelta(hours=8)))
            # 格式化为字符串
            utc8_times.append(dt_utc8.strftime('%Y-%m-%d %H:%M:%S'))
        
        # 插入时间列
        export_df.insert(1, 'time_utc8', utc8_times)
        
        # 确保timestamp是整数类型
        export_df['timestamp'] = export_df['timestamp'].astype('int64')
        
        # 转换为CSV
        output = StringIO()
        export_df.to_csv(output, index=False, encoding='utf-8-sig')
        csv_content = output.getvalue()
        
        # 生成文件名
        addr_short = address[:10] if len(address) > 10 else address
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{addr_short}_NetValue_{interval}_{current_time}.csv"
        
        # 返回CSV文件
        return Response(
            csv_content,
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename={filename}',
                'Content-Type': 'text/csv; charset=utf-8-sig'
            }
        )
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

