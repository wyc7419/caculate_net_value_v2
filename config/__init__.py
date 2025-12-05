# -*- coding: utf-8 -*-
"""
配置模块
=======

统一管理所有配置信息
"""
from .settings import *
from .database import *
from .data_source import *

__all__ = [
    # 路径配置
    'BASE_DIR', 'SQLITE_DB_PATH', 'OUTPUT_DIR', 
    'CSV_OUTPUT_DIR', 'CHART_OUTPUT_DIR',
    
    # 计算配置
    'DEFAULT_INTERVAL', 'DEBUG_MODE', 'ENABLE_CSV_EXPORT',
    'ENABLE_CHART_EXPORT', 'CHART_DPI', 'PLOT_FROM_FIRST_TRADE',
    'SUPPORTED_INTERVALS',
    
    # Web配置
    'WEB_HOST', 'WEB_PORT', 'WEB_DEBUG', 'ENABLE_ADDRESS_PRELOAD',
    
    # 数据库配置
    'TIMESCALE_CONFIG', 'DB_POOL_MIN_SIZE', 'DB_POOL_MAX_SIZE',
    
    # API配置
    'API_BASE_URL', 'API_VERSION', 'API_ENDPOINT', 'API_TIMEOUT',
    'API_MAX_RETRIES', 'API_RETRY_DELAY', 'API_ENABLE_CACHE', 'API_CACHE_TTL',
    'TRADES_API_ENDPOINT', 'FUNDING_API_ENDPOINT', 'LEDGER_API_ENDPOINT',
    'ACCOUNT_SNAPSHOT_API_ENDPOINT', 'POSITION_SNAPSHOT_API_ENDPOINT',
    'KLINE_API_ENDPOINT',
    'TRADES_API_BASE_URL', 'TRADES_API_TIMEOUT',  # 向后兼容
    'API_DEBUG', 'API_DEBUG_RESPONSE',
    'DATA_SOURCE_TYPE', 'DATA_SOURCE_PRIORITY',
    'validate_api_config', 'print_api_config',
]


