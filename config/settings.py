# -*- coding: utf-8 -*-
"""
主配置文件
=========

所有业务配置集中在这里
"""
import os

# ==================== 路径配置 ====================
# 项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 数据源路径
SQLITE_DB_PATH = os.getenv(
    'SQLITE_DB_PATH',
    r'D:\Desktop\caculate_net_value\HyperDataCollector--v6.3\data\sqlite\hyper_data.db'
)

# 输出目录
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
CSV_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'csv')
CHART_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'charts')

# 确保目录存在
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)

# ==================== 计算配置 ====================
# 默认计算参数
DEFAULT_INTERVAL = '1h'         # 默认时间区间
DEBUG_MODE = True               # 是否显示详细日志
ENABLE_CSV_EXPORT = False       # 是否导出CSV
ENABLE_CHART_EXPORT = False     # 是否生成图表
CHART_DPI = 150                 # 图表分辨率
PLOT_FROM_FIRST_TRADE = True    # 是否从第一笔交易开始绘图

# 支持的时间区间（仅支持小时级和日级）
SUPPORTED_INTERVALS = ['1h', '2h', '4h', '8h', '12h', '1d']

# ==================== Web服务配置 ====================
WEB_HOST = '0.0.0.0'
WEB_PORT = 5000
WEB_DEBUG = True
ENABLE_ADDRESS_PRELOAD = True   # 是否预加载地址列表

