# -*- coding: utf-8 -*-
"""
统计信息 API
============

提供数据库统计信息接口
"""
from flask import jsonify
from . import api_bp


@api_bp.route('/stats/<interval>')
def get_all_stats(interval):
    """
    获取指定时间区间所有地址的统计信息
    
    Args:
        interval: 时间区间（如 1h, 1d）
    
    Returns:
        JSON响应，包含表统计信息
    """
    from web.app import db_manager
    
    try:
        stats = db_manager.get_table_stats(interval)
        return jsonify({
            'success': True,
            'data': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

