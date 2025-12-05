# -*- coding: utf-8 -*-
"""
NetValueTimescaleManager 测试脚本
=================================

演示如何查询和管理 TimescaleDB 净值数据
"""

import sys
import os

# 添加模块路径
script_dir = os.path.dirname(os.path.abspath(__file__))
main_dir = os.path.join(os.path.dirname(script_dir), 'main')
sys.path.insert(0, main_dir)
sys.path.insert(0, os.path.dirname(script_dir))

from net_value_timescale_manager import NetValueTimescaleManager  # type: ignore
from config_timescale import TIMESCALE_CONFIG  # type: ignore

# 配置参数
ADDRESS = "0x0000000afcd4de376f2bf0094cdd01712f125995"
INTERVAL = '1h'

print("="*80)
print("NetValueTimescaleManager 测试脚本")
print("="*80)

print(f"\n📋 配置信息:")
print(f"   数据库地址: {TIMESCALE_CONFIG['host']}:{TIMESCALE_CONFIG['port']}")
print(f"   数据库名称: {TIMESCALE_CONFIG['database']}")
print(f"   查询地址: {ADDRESS}")
print(f"   时间区间: {INTERVAL}")

try:
    # 创建数据库管理器
    print("\n" + "="*80)
    print("创建数据库管理器...")
    print("="*80)
    
    db_manager = NetValueTimescaleManager(**TIMESCALE_CONFIG)
    print("✅ 数据库管理器创建成功")
    
    # 获取表统计信息
    print("\n" + "="*80)
    print(f"获取表统计信息 (net_value_{INTERVAL})...")
    print("="*80)
    
    stats = db_manager.get_table_stats(INTERVAL)
    
    if stats['exists']:
        print(f"✅ 表存在")
        print(f"   总记录数: {stats['total_records']}")
        print(f"   地址数量: {stats['address_count']}")
        print(f"   分块数: {stats['chunks']}")
        print(f"   压缩块数: {stats['compressed_chunks']}")
        print(f"   总大小: {stats['total_size']}")
        print(f"   压缩后大小: {stats['compressed_size']}")
        
        if stats['earliest_timestamp']:
            from datetime import datetime
            earliest = datetime.fromtimestamp(stats['earliest_timestamp'] / 1000)
            latest = datetime.fromtimestamp(stats['latest_timestamp'] / 1000)
            print(f"   时间范围: {earliest} 至 {latest}")
    else:
        print(f"⚠️  表不存在，请先运行 run_calculate_net_value_v2.py 生成数据")
        sys.exit(0)
    
    # 列出所有地址
    print("\n" + "="*80)
    print("列出所有地址...")
    print("="*80)
    
    addresses = db_manager.list_addresses(INTERVAL)
    
    if addresses:
        print(f"✅ 共有 {len(addresses)} 个地址:")
        for i, addr in enumerate(addresses[:10], 1):  # 只显示前10个
            print(f"   {i}. {addr}")
        if len(addresses) > 10:
            print(f"   ... 还有 {len(addresses) - 10} 个地址")
    else:
        print(f"ℹ️  没有找到任何地址")
    
    # 查询指定地址的最新时间戳
    print("\n" + "="*80)
    print(f"查询地址的最新时间戳...")
    print("="*80)
    
    latest_timestamp = db_manager.get_latest_timestamp(ADDRESS, INTERVAL)
    
    if latest_timestamp:
        from datetime import datetime
        latest_time = datetime.fromtimestamp(latest_timestamp / 1000)
        print(f"✅ 最新时间戳: {latest_timestamp}")
        print(f"   时间: {latest_time}")
    else:
        print(f"ℹ️  该地址没有数据")
        sys.exit(0)
    
    # 查询净值数据
    print("\n" + "="*80)
    print("查询净值数据...")
    print("="*80)
    
    df = db_manager.query_net_value_data(ADDRESS, INTERVAL)
    
    if len(df) > 0:
        print(f"✅ 查询成功，共 {len(df)} 条记录")
        print(f"\n前5条数据:")
        print(df.head(5).to_string())
        
        print(f"\n最后5条数据:")
        print(df.tail(5).to_string())
        
        # 显示统计信息
        print(f"\n数据统计:")
        print(f"   时间范围: {df.iloc[0]['timestamp']} 至 {df.iloc[-1]['timestamp']}")
        print(f"   净值范围: {df['net_value'].min():.6f} 至 {df['net_value'].max():.6f}")
        print(f"   总资产范围: ${df['total_assets'].min():,.2f} 至 ${df['total_assets'].max():,.2f}")
        print(f"   累计PnL范围: ${df['cumulative_pnl'].min():,.2f} 至 ${df['cumulative_pnl'].max():,.2f}")
        
        # TimescaleDB 特有信息
        print(f"\n📊 TimescaleDB 优势:")
        print(f"   ✅ 数据已按时间自动分区")
        print(f"   ✅ 旧数据自动压缩（节省 {stats['compressed_chunks']}/{stats['chunks']} 块）")
        print(f"   ✅ 查询只扫描相关时间范围的分区")
        print(f"   ✅ 支持高并发读写")
    else:
        print(f"ℹ️  该地址没有数据")
    
    print("\n" + "="*80)
    print("✅ 测试完成！")
    print("="*80)
    
    print(f"\n💡 提示:")
    print(f"   - 可以在 Timescale Cloud 控制台查看数据")
    print(f"   - 控制台地址: https://console.cloud.timescale.com")
    print(f"   - 或使用 Python 查询数据")

except Exception as e:
    print(f"\n❌ 错误: {e}")
    import traceback
    traceback.print_exc()
    
    print(f"\n💡 提示:")
    print(f"   1. 确保已注册 Timescale Cloud: https://console.cloud.timescale.com")
    print(f"   2. 检查连接配置: config_timescale.py")
    print(f"   3. 先运行计算生成数据: python run_calculate_net_value_v2.py")

