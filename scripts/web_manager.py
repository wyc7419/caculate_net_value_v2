# -*- coding: utf-8 -*-
"""
Web 服务管理脚本
===============

启动和停止Web可视化服务
"""
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def start_web(port=None):
    """启动Web服务"""
    from config import WEB_HOST, WEB_PORT, WEB_DEBUG
    from web.app import app
    
    actual_port = port or WEB_PORT
    
    print(f"\n{'='*60}")
    print(f"🌐 启动Web可视化服务")
    print(f"{'='*60}")
    print(f"\n访问地址: http://localhost:{actual_port}")
    print(f"监听地址: {WEB_HOST}:{actual_port}")
    print(f"\n按 Ctrl+C 停止服务\n")
    print(f"{'='*60}\n")
    
    app.run(debug=WEB_DEBUG, host=WEB_HOST, port=actual_port)


def stop_web():
    """停止Web服务"""
    import psutil
    import signal
    
    print("\n🔍 查找运行中的Flask进程...")
    
    stopped_count = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and any('app.py' in str(arg) for arg in cmdline):
                print(f"   找到进程: PID={proc.info['pid']}, 命令={' '.join(cmdline)}")
                
                # 发送终止信号
                os.kill(proc.info['pid'], signal.SIGTERM)
                print(f"   ✅ 已发送停止信号到进程 {proc.info['pid']}")
                stopped_count += 1
        
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    if stopped_count > 0:
        print(f"\n✅ 已停止 {stopped_count} 个Web服务进程")
    else:
        print("\n⚠️ 没有找到运行中的Web服务")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Web服务管理')
    parser.add_argument('action', choices=['start', 'stop'], help='启动或停止服务')
    parser.add_argument('--port', type=int, help='端口号（仅用于start）')
    
    args = parser.parse_args()
    
    if args.action == 'start':
        start_web(port=args.port)
    else:
        stop_web()

