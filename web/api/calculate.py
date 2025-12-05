# -*- coding: utf-8 -*-
"""
净值计算 API
============

提供净值计算接口，支持实时日志流
"""
from flask import Response, jsonify, stream_with_context
import json
import sys
import io
from threading import Thread
from queue import Queue
from . import api_bp


@api_bp.route('/check-data/<interval>/<address>')
def check_data_exists(interval, address):
    """
    检查指定地址和时间区间的数据是否存在（快速检查，仅查询更新记录表）
    
    Args:
        interval: 时间区间
        address: 账户地址
    
    Returns:
        JSON响应，包含数据是否存在
    """
    from web.app import db_manager
    
    try:
        # 使用快速检查方法（只查询更新记录表）
        result = db_manager.check_data_exists(address, interval)
        
        return jsonify({
            'success': True,
            'exists': result['exists'],
            'last_update': result['last_update']
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@api_bp.route('/calculate/<interval>/<address>')
def calculate_net_value(interval, address):
    """
    计算指定地址和时间区间的净值，实时返回日志
    
    Args:
        interval: 时间区间
        address: 账户地址
        
    Query Params:
        force_overwrite: 是否强制全量覆盖（默认 false）
    
    Returns:
        SSE流，实时推送计算日志
    """
    from flask import request
    
    # 解析 force_overwrite 参数
    force_overwrite_param = request.args.get('force_overwrite', 'false').strip().lower()
    force_overwrite = force_overwrite_param in ['true', '1', 'yes']
    
    def generate():
        """生成器函数，用于SSE流"""
        # 创建一个队列用于接收日志
        log_queue = Queue()
        
        # 自定义的输出捕获类（Web环境专用）
        class LogCapture:
            def __init__(self, queue):
                self.queue = queue
                self.buffer = []
                self.closed = False
            
            def write(self, text):
                # 只写入队列供Web端显示（不写回原始stdout）
                if not self.closed and text and text.strip():
                    try:
                        self.queue.put(text)
                    except:
                        pass
            
            def flush(self):
                pass  # Web环境中不需要刷新
            
            def close(self):
                self.closed = True
        
        # 保存真正的原始stdout（在模块加载之前的）
        import sys
        if not hasattr(sys, '__stdout__'):
            sys.__stdout__ = sys.stdout
            sys.__stderr__ = sys.stderr
        
        original_stdout = sys.__stdout__
        original_stderr = sys.__stderr__
        
        # 创建日志捕获对象
        log_capture = LogCapture(log_queue)
        
        # 提前导入计算函数（在重定向stdout之前）
        from scripts.calculate import calculate_net_value as calc_func
        
        def run_calculation():
            """在线程中运行计算"""
            try:
                # 重定向stdout和stderr
                sys.stdout = log_capture
                sys.stderr = log_capture
                
                # 执行计算（禁用CSV和图表，启用数据库保存）
                # incremental=False 表示全量覆盖
                result = calc_func(
                    address=address,
                    interval=interval,
                    enable_csv=False,
                    enable_plot=False,
                    save_to_db=True,
                    incremental=not force_overwrite  # force_overwrite=True 时 incremental=False
                )
                
                # 发送完成信号
                if result is not None:
                    # 计算成功，更新缓存
                    from web.app import address_cache
                    
                    # 确保该时间区间的缓存列表存在
                    if interval not in address_cache:
                        address_cache[interval] = []
                    
                    # 如果地址不在缓存中，添加它
                    if address not in address_cache[interval]:
                        address_cache[interval].append(address)
                        log_queue.put(f"\n✅ 已将地址添加到 {interval} 缓存中\n")
                    
                    log_queue.put("__CALCULATION_SUCCESS__")
                else:
                    log_queue.put("__CALCULATION_FAILED__")
                    
            except Exception as e:
                import traceback
                try:
                    error_msg = f"❌ 计算失败: {str(e)}\n{traceback.format_exc()}"
                    log_queue.put(error_msg)
                except:
                    # 如果连写入队列都失败了，至少记录到原始 stderr
                    original_stderr.write(f"Critical error: {str(e)}\n")
                log_queue.put("__CALCULATION_FAILED__")
            finally:
                # 关闭日志捕获
                try:
                    log_capture.close()
                except:
                    pass
                
                # 恢复原始stdout和stderr
                try:
                    sys.stdout = original_stdout
                    sys.stderr = original_stderr
                except:
                    # 如果恢复失败，使用保存的原始值
                    sys.stdout = sys.__stdout__
                    sys.stderr = sys.__stderr__
                
                log_queue.put("__STREAM_END__")
        
        # 启动计算线程
        calc_thread = Thread(target=run_calculation, daemon=True)
        calc_thread.start()
        
        # 流式发送日志
        try:
            while True:
                log_msg = log_queue.get()
                
                if log_msg == "__STREAM_END__":
                    break
                elif log_msg == "__CALCULATION_SUCCESS__":
                    yield f"data: {json.dumps({'type': 'complete', 'success': True})}\n\n"
                elif log_msg == "__CALCULATION_FAILED__":
                    yield f"data: {json.dumps({'type': 'complete', 'success': False})}\n\n"
                else:
                    # 发送日志消息
                    yield f"data: {json.dumps({'type': 'log', 'message': log_msg})}\n\n"
                    
        except GeneratorExit:
            # 客户端断开连接
            pass
    
    # 返回SSE响应
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )

