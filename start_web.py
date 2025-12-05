#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
启动Web服务 - 简单入口
======================

直接修改下面的参数，然后运行：
    python start_web.py

无需命令行参数！
"""
import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ==================== 在这里修改参数 ====================

# 监听地址（0.0.0.0表示所有网络接口，127.0.0.1表示仅本地）
HOST = "0.0.0.0"

# 监听端口
PORT = 5000

# 调试模式（生产环境请设为False）
DEBUG = True

# =======================================================


def main():
    """启动Web服务"""
    from web.app import app
    
    print("\n" + "="*60)
    print("净值可视化 Web 应用")
    print("="*60)
    print(f"\n访问地址: http://localhost:{PORT}")
    print(f"监听地址: {HOST}:{PORT}")
    print(f"调试模式: {DEBUG}")
    print("\n按 Ctrl+C 停止服务\n")
    print("="*60 + "\n")
    
    # 启动应用
    app.run(debug=DEBUG, host=HOST, port=PORT)


if __name__ == '__main__':
    main()

 