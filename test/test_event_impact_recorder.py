"""
简单测试脚本 - EventImpactRecorder（从 API 获取数据）
"""

import sys
import os

# 添加模块路径
script_dir = os.path.dirname(os.path.abspath(__file__))
main_dir = os.path.join(os.path.dirname(script_dir), 'main')
sys.path.insert(0, main_dir)

from event_impact_recorder import EventImpactRecorder  # type: ignore

# 配置参数
address = "0x15b325660a1c4a9582a7d834c31119c0cb9e3a42"

print(f"测试 EventImpactRecorder")
print(f"账户地址: {address}")

# 创建记录器（从 API 加载数据）
print("\n创建记录器...")
recorder = EventImpactRecorder(address)

# 处理事件
print("处理所有事件...")
impacts = recorder.process_all_events()
print(f"共 {len(impacts)} 个事件")

# 创建输出文件夹
output_dir = os.path.join(os.path.dirname(__file__), "out_test_event_impact_recorder")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 导出CSV
output_csv = os.path.join(output_dir, f"{address[:10]}_test_output.csv")
recorder.export_to_csv(output_csv)
print(f"完成: {output_csv}")
