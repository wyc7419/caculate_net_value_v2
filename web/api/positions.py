# -*- coding: utf-8 -*-
"""
过去持仓计算API
"""

from flask import Blueprint, Response, jsonify, request, send_file, after_this_request
import os
import sys
from datetime import datetime
import io
import threading
import pandas as pd
import time
import json

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from main.calculate_positions_backward import PositionBackwardCalculator

positions_api_bp = Blueprint('positions_api', __name__)

# 存储计算结果（DataFrame）和文件名（内存中临时保存）
# 结构：{address: {'dataframe': df, 'filename': str, 'timestamp': str, 'created_at': float}}
# 说明：
# - 数据保存在内存中，不写入磁盘
# - 下载完成后自动删除
# - 超过10分钟未下载也会自动清理
# - 服务器重启时自动清空
calculation_results = {}

# 数据保留时间（秒）
DATA_RETENTION_SECONDS = 600  # 10分钟


def cleanup_expired_results():
    """
    清理超过10分钟未下载的计算结果
    """
    now = time.time()
    expired_addresses = []
    
    for address, result_info in list(calculation_results.items()):
        created_at = result_info.get('created_at', now)
        age = now - created_at
        
        if age > DATA_RETENTION_SECONDS:
            expired_addresses.append(address)
    
    for address in expired_addresses:
        del calculation_results[address]
        print(f"🗑️  已清理过期数据: {address}（超过10分钟未下载）", flush=True)
    
    return len(expired_addresses)


def start_cleanup_thread():
    """
    启动后台清理线程，每5分钟检查一次过期数据
    """
    def cleanup_loop():
        while True:
            time.sleep(300)  # 5分钟检查一次
            try:
                count = cleanup_expired_results()
                if count > 0:
                    print(f"✅ 定期清理: 删除了 {count} 个过期计算结果", flush=True)
            except Exception as e:
                print(f"❌ 清理任务出错: {e}", flush=True)
    
    thread = threading.Thread(target=cleanup_loop, daemon=True, name="PositionsCleanup")
    thread.start()
    print("🚀 持仓数据自动清理线程已启动（10分钟过期，每5分钟检查一次）", flush=True)


# 启动清理线程（模块加载时自动启动）
start_cleanup_thread()


class LogCapture:
    """捕获stdout和stderr输出"""
    def __init__(self):
        self.logs = []
        self.closed = False
    
    def write(self, message):
        if not self.closed and message.strip():
            self.logs.append(message)
    
    def flush(self):
        pass
    
    def close(self):
        self.closed = True
    
    def get_logs(self):
        return self.logs.copy()


def run_positions_calculation(address: str, log_capture: LogCapture):
    """
    在单独线程中运行持仓计算（仅计算，不导出文件）
    """
    # 重定向stdout和stderr
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    
    try:
        sys.stdout = log_capture
        sys.stderr = log_capture
        
        # 执行计算（不导出CSV）
        calculator = PositionBackwardCalculator(address, export_csv=False)
        df_result = calculator.calculate_backward(output_csv_path=None)
        
        if df_result is not None:
            # 将结果保存到内存
            address_prefix = address[:10] if len(address) >= 10 else address
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{address_prefix}_positions_{timestamp}.csv"
            
            calculation_results[address] = {
                'dataframe': df_result,
                'filename': filename,
                'timestamp': timestamp,
                'created_at': time.time()  # 记录创建时间（用于过期检查）
            }
            
            log_capture.write(f"\n✅ 计算完成！\n")
            log_capture.write(f"✅ 共处理 {len(df_result)} 条记录\n")
            log_capture.write(f"💡 请在10分钟内下载，超时将自动清理\n")
        else:
            log_capture.write("\n❌ 计算失败\n")
            
    except Exception as e:
        log_capture.write(f"\n❌ 计算失败: {str(e)}\n")
    finally:
        # 恢复stdout和stderr
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_capture.close()


@positions_api_bp.route('/api/positions/export', methods=['POST'])
def export_positions():
    """
    导出过去持仓CSV
    
    请求参数:
        address: 账户地址
    
    返回:
        SSE流，实时返回计算日志
    """
    data = request.get_json()
    address = data.get('address', '').strip().lower()
    
    if not address:
        return jsonify({'success': False, 'error': '请提供账户地址'}), 400
    
    # 清理该地址的旧计算结果（如果存在）
    if address in calculation_results:
        del calculation_results[address]
    
    # 创建日志捕获器
    log_capture = LogCapture()
    
    # 在后台线程中运行计算
    thread = threading.Thread(target=run_positions_calculation, args=(address, log_capture))
    thread.daemon = True
    thread.start()
    
    def generate():
        """生成SSE流"""
        last_log_count = 0
        
        try:
            # 发送初始消息
            initial_data = json.dumps({'type': 'log', 'message': '开始计算过去持仓...\n'})
            yield f"data: {initial_data}\n\n"
            
            # 持续发送日志
            while thread.is_alive() or last_log_count < len(log_capture.logs):
                logs = log_capture.get_logs()
                
                # 发送新日志
                for i in range(last_log_count, len(logs)):
                    message = logs[i]
                    log_data = json.dumps({'type': 'log', 'message': message})
                    yield f"data: {log_data}\n\n"
                
                last_log_count = len(logs)
                
                if thread.is_alive():
                    import time
                    time.sleep(0.1)
            
            # 检查是否成功
            if address in calculation_results:
                result_info = calculation_results[address]
                complete_data = json.dumps({
                    'type': 'complete', 
                    'success': True, 
                    'filename': result_info['filename']
                })
                yield f"data: {complete_data}\n\n"
            else:
                fail_data = json.dumps({'type': 'complete', 'success': False, 'error': '计算失败'})
                yield f"data: {fail_data}\n\n"
                
        except Exception as e:
            error_data = json.dumps({'type': 'error', 'message': f'发生错误: {str(e)}'})
            yield f"data: {error_data}\n\n"
    
    return Response(generate(), mimetype='text/event-stream')


@positions_api_bp.route('/api/positions/download/<address>', methods=['GET'])
def download_positions(address):
    """
    下载生成的CSV文件（从内存中的DataFrame直接生成）
    
    参数:
        address: 账户地址
    
    返回:
        CSV文件（直接下载，不保存到服务器）
    """
    address = address.strip().lower()
    
    if address not in calculation_results:
        return jsonify({'success': False, 'error': '数据不存在，请先进行计算'}), 404
    
    result_info = calculation_results[address]
    df = result_info['dataframe']
    filename = result_info['filename']
    
    # 将 DataFrame 转换为 CSV 字符串（内存中）
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, encoding='utf-8-sig', index=False)
    csv_buffer.seek(0)
    
    # 转换为字节流
    csv_bytes = io.BytesIO(csv_buffer.getvalue().encode('utf-8-sig'))
    csv_bytes.seek(0)
    
    # 直接返回文件流，不保存到磁盘
    response = send_file(
        csv_bytes,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )
    
    # 下载完成后清理内存（释放 DataFrame）
    # 注意：使用 after_this_request 确保响应发送完成后再清理
    @after_this_request
    def cleanup(response):
        if address in calculation_results:
            del calculation_results[address]
            print(f"✅ 已清理地址 {address} 的计算结果（下载完成）", flush=True)
        return response
    
    return response

