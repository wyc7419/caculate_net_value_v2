# -*- coding: utf-8 -*-
"""
数据库配置
=========

TimescaleDB 和其他数据库连接配置
"""
import os

# 尝试加载环境变量（如果安装了python-dotenv）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # 如果没有安装python-dotenv，直接使用os.getenv

# TimescaleDB 配置
TIMESCALE_CONFIG = {
    'host': os.getenv('TIMESCALE_HOST', 'ube2foqvft.om3uo4yni9.tsdb.cloud.timescale.com'),
    'port': int(os.getenv('TIMESCALE_PORT', '39839')),
    'database': os.getenv('TIMESCALE_DB', 'tsdb'),
    'user': os.getenv('TIMESCALE_USER', 'tsdbadmin'),
    'password': os.getenv('TIMESCALE_PASSWORD', 'gxe2xu9cgs60a5d2'),  # 默认值，建议使用环境变量
}

# 连接池配置（预留，暂未使用）
DB_POOL_MIN_SIZE = 1
DB_POOL_MAX_SIZE = 10

