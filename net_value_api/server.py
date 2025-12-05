# -*- coding: utf-8 -*-
"""
净值数据 API 服务器
===================

独立运行的 API 服务，供第三方系统调用

启动方式：
    python net_value_api/server.py [--port 8080] [--host 0.0.0.0]

接口列表：
    GET /netvalue/intervals              - 查询所有可用时间周期
    GET /netvalue/data/<interval>/<address> - 查询净值数据
"""

import sys
import os
import argparse

# 设置编码
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask
from flask_cors import CORS
from net_value_api.api import net_value_api_bp


def create_app():
    """创建 Flask 应用"""
    app = Flask(__name__)
    CORS(app)  # 允许跨域请求
    
    # 注册净值数据 API 蓝图
    app.register_blueprint(net_value_api_bp)
    
    # 添加根路由
    @app.route('/')
    def index():
        return {
            'service': '净值数据 API',
            'version': '1.0.0',
            'endpoints': {
                'intervals': 'GET /netvalue/intervals',
                'data': 'GET /netvalue/data/<interval>/<address>'
            }
        }
    
    return app


def main():
    parser = argparse.ArgumentParser(description='外部 API 服务器')
    parser.add_argument('--host', default='0.0.0.0', help='监听地址 (默认: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8080, help='监听端口 (默认: 8080)')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    
    args = parser.parse_args()
    
    app = create_app()
    
    print("\n" + "=" * 60)
    print("净值数据 API 服务")
    print("=" * 60)
    print(f"\n访问地址: http://{args.host}:{args.port}")
    print(f"\n接口列表:")
    print(f"  GET /netvalue/intervals")
    print(f"      - 查询所有可用时间周期")
    print(f"  GET /netvalue/data/<interval>/<address>")
    print(f"      - 查询指定地址的净值数据")
    print(f"\n示例:")
    print(f"  curl http://localhost:{args.port}/netvalue/intervals")
    print(f"  curl http://localhost:{args.port}/netvalue/data/1h/0x123...")
    print("\n按 Ctrl+C 停止服务\n")
    
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()

