# -*- coding: utf-8 -*-
"""
数据源配置文件
==============

配置从外部 API 获取数据的相关参数
（交易数据、资金费、账本、快照等）

注意：这是获取数据用的外部 API 配置，不是对外提供的 API
"""
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# ==================== 基础 API 配置 ====================

# API 基础 URL
API_BASE_URL = os.getenv(
    'API_BASE_URL',
    'http://65.49.235.55:8000'
)

# API 版本
API_VERSION = 'v1'

# 完整 API 端点
API_ENDPOINT = f"{API_BASE_URL}/api/{API_VERSION}"

# ==================== 请求配置 ====================

# API 请求超时时间（秒）
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '30'))

# 最大重试次数
API_MAX_RETRIES = int(os.getenv('API_MAX_RETRIES', '3'))

# 重试延迟（秒）
API_RETRY_DELAY = int(os.getenv('API_RETRY_DELAY', '1'))

# 是否启用请求缓存
API_ENABLE_CACHE = os.getenv('API_ENABLE_CACHE', 'false').lower() in ('true', '1', 'yes')

# 缓存过期时间（秒）
API_CACHE_TTL = int(os.getenv('API_CACHE_TTL', '300'))  # 5分钟

# ==================== 数据接口配置 ====================

# 交易数据 API 端点
TRADES_API_ENDPOINT = f"{API_ENDPOINT}/trades/query"

# 资金费数据 API 端点
FUNDING_API_ENDPOINT = f"{API_ENDPOINT}/ledger/funding"

# 账本数据 API 端点
LEDGER_API_ENDPOINT = f"{API_ENDPOINT}/ledger/query"

# 账户快照 API 端点（未来）
ACCOUNT_SNAPSHOT_API_ENDPOINT = f"{API_ENDPOINT}/accounts/snapshot"

# 持仓快照 API 端点（未来）
POSITION_SNAPSHOT_API_ENDPOINT = f"{API_ENDPOINT}/positions/snapshot"

# K线数据 API 端点（未来）
KLINE_API_ENDPOINT = f"{API_ENDPOINT}/kline/query"

# ==================== 兼容性配置 ====================

# 旧配置名称（向后兼容）
TRADES_API_BASE_URL = API_BASE_URL
TRADES_API_TIMEOUT = API_TIMEOUT

# ==================== 调试配置 ====================

# 是否打印 API 请求详情
API_DEBUG = os.getenv('API_DEBUG', 'false').lower() in ('true', '1', 'yes')

# 是否打印 API 响应详情
API_DEBUG_RESPONSE = os.getenv('API_DEBUG_RESPONSE', 'false').lower() in ('true', '1', 'yes')

# ==================== 数据源优先级配置 ====================

# 数据源类型：'api' 或 'database' 或 'hybrid'
DATA_SOURCE_TYPE = os.getenv('DATA_SOURCE_TYPE', 'api')

# 混合模式下的数据源优先级
# 'api_first': 优先使用 API，失败时降级到数据库
# 'database_first': 优先使用数据库，失败时降级到 API
DATA_SOURCE_PRIORITY = os.getenv('DATA_SOURCE_PRIORITY', 'api_first')

# ==================== 配置验证 ====================

def validate_api_config():
    """验证 API 配置"""
    errors = []
    
    if not API_BASE_URL:
        errors.append("API_BASE_URL 未配置")
    
    if API_TIMEOUT <= 0:
        errors.append(f"API_TIMEOUT 必须大于0，当前值: {API_TIMEOUT}")
    
    if API_MAX_RETRIES < 0:
        errors.append(f"API_MAX_RETRIES 必须大于等于0，当前值: {API_MAX_RETRIES}")
    
    if errors:
        error_msg = "\n".join(f"  - {err}" for err in errors)
        raise ValueError(f"API 配置错误:\n{error_msg}")
    
    return True


# ==================== 配置信息 ====================

def print_api_config():
    """打印 API 配置信息（用于调试）"""
    print("="*60)
    print("API 配置信息")
    print("="*60)
    print(f"API 基础 URL: {API_BASE_URL}")
    print(f"API 版本: {API_VERSION}")
    print(f"API 端点: {API_ENDPOINT}")
    print(f"超时时间: {API_TIMEOUT} 秒")
    print(f"最大重试: {API_MAX_RETRIES} 次")
    print(f"数据源类型: {DATA_SOURCE_TYPE}")
    print(f"数据源优先级: {DATA_SOURCE_PRIORITY}")
    print(f"调试模式: {API_DEBUG}")
    print("="*60)


# 自动验证配置（导入时执行）
try:
    validate_api_config()
except ValueError as e:
    print(f"[WARNING] {e}")


if __name__ == '__main__':
    # 测试配置
    print_api_config()

