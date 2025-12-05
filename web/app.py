# -*- coding: utf-8 -*-
"""
净值可视化 Web 应用 - Flask 后端
================================

提供 RESTful API 接口，从 TimescaleDB 读取数据供前端展示
"""

import sys
import os

# 设置输出编码为UTF-8（解决Windows GBK编码问题）
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main.net_value_timescale_manager import NetValueTimescaleManager
from config import TIMESCALE_CONFIG, ENABLE_ADDRESS_PRELOAD

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 初始化数据库管理器
db_manager = NetValueTimescaleManager(**TIMESCALE_CONFIG)

# 全局缓存：存储每个时间区间的地址列表
# 结构：{'1m': ['addr1', 'addr2'], '1h': ['addr3'], ...}
address_cache = {}

# 配置：是否启用预加载（从配置文件读取，也可通过环境变量 DISABLE_PRELOAD=1 禁用）
ENABLE_PRELOAD = ENABLE_ADDRESS_PRELOAD and os.environ.get('DISABLE_PRELOAD', '').lower() not in ('1', 'true', 'yes')

def preload_addresses():
    """
    预加载所有时间区间的地址列表到内存（后台执行）
    """
    import time
    import threading
    
    def _load():
        global address_cache
        
        start_time = time.time()
        
        print("\n" + "="*60, flush=True)
        print("🚀 后台预加载地址列表...", flush=True)
        print("="*60, flush=True)
        
        try:
            # 使用批量查询方法，一次性获取所有时间区间的地址（性能优化）
            all_addresses = db_manager.list_all_addresses()
            address_cache.update(all_addresses)
            
            # 显示加载结果
            total_addresses = 0
            for idx, (interval, addresses) in enumerate(sorted(all_addresses.items()), 1):
                total_addresses += len(addresses)
                print(f"  [{idx:2d}/{len(all_addresses)}] ✅ {interval:4s}: {len(addresses):3d} 个地址", flush=True)
            
            elapsed = time.time() - start_time
            print("="*60, flush=True)
            print(f"✅ 后台预加载完成！共 {len(address_cache)} 个时间区间，{total_addresses} 个地址", flush=True)
            print(f"⏱️  耗时: {elapsed:.2f} 秒", flush=True)
            print("="*60, flush=True)
            print(flush=True)
            
        except Exception as e:
            print(f"❌ 预加载失败: {e}", flush=True)
            import traceback
            traceback.print_exc()
            # 失败时初始化空缓存
            for interval in db_manager.INTERVAL_TABLE_MAP.keys():
                address_cache[interval] = []
    
    # 在后台线程中执行预加载
    thread = threading.Thread(target=_load, daemon=True, name="AddressPreloader")
    thread.start()
    print("✨ 地址预加载已在后台启动，您可以立即访问网页！", flush=True)

# 标记是否已经启动预加载（避免重复启动）
preload_started = False

@app.before_request
def start_preload_once():
    """
    在第一个请求到达前启动后台预加载
    这样应用可以快速启动，用户可以立即访问网页
    """
    global preload_started
    
    if not preload_started and ENABLE_PRELOAD:
        preload_started = True
        preload_addresses()


# ==================== 路由 ====================

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


# ==================== 注册 API Blueprint ====================
from .api import api_bp
from .api.positions import positions_api_bp
app.register_blueprint(api_bp)
app.register_blueprint(positions_api_bp)


if __name__ == '__main__':
    print("\n" + "="*60, flush=True)
    print("净值可视化 Web 应用", flush=True)
    print("="*60, flush=True)
    print(f"\n访问地址: http://localhost:5000", flush=True)
    print(f"数据库: {TIMESCALE_CONFIG['host']}", flush=True)
    
    if ENABLE_PRELOAD:
        print(f"\n💡 地址列表将在首次访问时在后台加载", flush=True)
        print(f"   您可以立即打开网页，后台加载不影响访问", flush=True)
    else:
        print(f"\n💡 地址列表将在访问时按需加载", flush=True)
    
    print("\n按 Ctrl+C 停止服务\n", flush=True)
    
    app.run(debug=True, host='0.0.0.0', port=5000)

