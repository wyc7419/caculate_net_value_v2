# -*- coding: utf-8 -*-
"""
时间区间 API
============

提供时间区间相关接口
"""
from flask import jsonify
from . import api_bp


@api_bp.route('/intervals')
def get_intervals():
    """
    获取所有可用的时间区间
    
    Returns:
        JSON响应，包含所有支持的时间区间列表
    """
    from web.app import db_manager
    
    # 从数据库管理器获取所有支持的时间区间
    intervals = list(db_manager.INTERVAL_TABLE_MAP.keys())
    
    return jsonify({
        'success': True,
        'data': intervals
    })

