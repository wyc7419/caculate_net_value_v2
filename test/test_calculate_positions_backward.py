# -*- coding: utf-8 -*-
"""
PositionBackwardCalculator 测试脚本
=====================================

功能：
1. 从 API 获取快照数据
2. 从 API 获取事件数据
3. 逐笔撤销事件，计算每笔事件前的持仓状态
4. 导出结果到CSV文件
"""

import sys
import os

# 添加项目根目录到 sys.path（支持包内相对导入）
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))  # caculate_net_value 的父目录
sys.path.insert(0, project_root)

from caculate_net_value.main.calculate_positions_backward import PositionBackwardCalculator

# 配置参数
ADDRESS = "0x06459273920defe761a706a9fa64a2e2fb3989de"  # 账户地址

print("="*80)
print("测试 PositionBackwardCalculator - 逐笔撤销事件计算持仓（从 API 获取数据）")
print("="*80)

print(f"\n📋 配置信息:")
print(f"   账户地址: {ADDRESS}")

# 创建输出文件夹
output_dir = os.path.join(script_dir, "out_test_calculate_positions_backward")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"\n✅ 已创建输出文件夹: {output_dir}")
else:
    print(f"\n📁 输出文件夹已存在: {output_dir}")

# 输出CSV文件路径
address_prefix = ADDRESS[:10] if len(ADDRESS) >= 10 else ADDRESS
OUTPUT_CSV = os.path.join(output_dir, f"{address_prefix}_positions_backward_test.csv")

print(f"   输出路径: {OUTPUT_CSV}")

try:
    # 创建计算器（启用CSV导出）
    print("\n" + "="*80)
    print("创建 PositionBackwardCalculator...")
    print("="*80)
    
    calculator = PositionBackwardCalculator(
        address=ADDRESS,
        export_csv=True  # 启用CSV导出
    )
    
    # 逐笔撤销事件，计算持仓（从最新快照开始）
    print("\n" + "="*80)
    print("开始逐笔撤销事件，计算历史持仓...")
    print("="*80)
    
    df_result = calculator.calculate_backward(OUTPUT_CSV)
    
    if df_result is not None and len(df_result) > 0:
        print("\n" + "="*80)
        print("✅ 测试完成！")
        print("="*80)
        
        print(f"\n📊 结果统计:")
        print(f"   事件总数: {len(df_result)}")
        print(f"   列数: {len(df_result.columns)}")
        print(f"   时间范围: {df_result.iloc[0]['time']} 至 {df_result.iloc[-1]['time']}")
        print(f"\n   CSV文件已保存到: {OUTPUT_CSV}")

    else:
        print("\n⚠️  警告: 未生成结果数据")

except Exception as e:
    print(f"\n❌ 错误: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("测试结束")
print("="*80)
