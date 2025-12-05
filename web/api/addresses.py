# -*- coding: utf-8 -*-
"""
地址 API
========

提供地址列表相关接口
"""
from flask import jsonify
from . import api_bp


@api_bp.route('/addresses/<interval>')
def get_addresses(interval):
    """
    获取指定时间区间的所有地址（从缓存读取）
    
    Args:
        interval: 时间区间（如 1h, 1d）
    
    Returns:
        JSON响应，包含地址列表
    """
    from web.app import db_manager, address_cache
    
    try:
        # 从缓存中获取地址列表
        if interval in address_cache:
            return jsonify({
                'success': True,
                'data': address_cache[interval],
                'cached': True  # 标记数据来自缓存
            })
        else:
            # 如果缓存中没有，则查询数据库并更新缓存
            addresses = db_manager.list_addresses(interval)
            address_cache[interval] = addresses
            return jsonify({
                'success': True,
                'data': addresses,
                'cached': False
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

