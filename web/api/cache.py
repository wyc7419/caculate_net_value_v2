# -*- coding: utf-8 -*-
"""
缓存管理 API
============

提供缓存管理相关接口
"""
from flask import jsonify
from . import api_bp


@api_bp.route('/refresh-cache', methods=['POST'])
def refresh_cache():
    """
    刷新地址缓存（同步执行）
    
    Returns:
        JSON响应，包含缓存刷新结果
    """
    from web.app import address_cache, db_manager
    import time
    
    try:
        start_time = time.time()
        
        # 使用批量查询方法，一次性刷新所有时间区间的地址
        all_addresses = db_manager.list_all_addresses()
        address_cache.clear()
        address_cache.update(all_addresses)
        
        elapsed = time.time() - start_time
        total_addresses = sum(len(addrs) for addrs in all_addresses.values())
        
        return jsonify({
            'success': True,
            'message': f'缓存刷新成功，耗时 {elapsed:.2f} 秒',
            'intervals': len(all_addresses),
            'total_addresses': total_addresses,
            'cache_info': {
                interval: len(addresses) 
                for interval, addresses in address_cache.items()
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@api_bp.route('/cache-info')
def get_cache_info():
    """
    获取缓存信息
    
    Returns:
        JSON响应，包含当前缓存状态
    """
    from web.app import address_cache
    
    try:
        cache_info = {
            'intervals': len(address_cache),
            'details': {
                interval: {
                    'address_count': len(addresses),
                    'addresses': addresses
                }
                for interval, addresses in address_cache.items()
            }
        }
        return jsonify({
            'success': True,
            'data': cache_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

