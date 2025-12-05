#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
净值计算 - 简单入口
==================

直接修改下面的参数，然后运行：
    python calc.py

无需命令行参数！
"""
import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ==================== 在这里修改参数 ====================

# 账户地址（必填）
ADDRESS = "0x06459273920defe761a706a9fa64a2e2fb3989de"

# 时间区间（可选：1h, 2h, 4h, 8h, 12h, 1d）
INTERVAL = "1d"

# 是否导出CSV文件
ENABLE_CSV = True

# 是否生成图表
ENABLE_CHART = False

# 是否保存到数据库
SAVE_TO_DB = True

# 图表分辨率（150=快速，300=高质量）
CHART_DPI = 150

# =======================================================


def main():
    """执行计算"""
    print("\n" + "="*60)
    print("净值计算")
    print("="*60)
    print(f"\n配置参数:")
    print(f"  地址: {ADDRESS}")
    print(f"  时间区间: {INTERVAL}")
    print(f"  导出CSV: {ENABLE_CSV}")
    print(f"  生成图表: {ENABLE_CHART}")
    print(f"  保存数据库: {SAVE_TO_DB}")
    print("\n" + "="*60 + "\n")
    
    # 导入计算函数
    from scripts.calculate import calculate_net_value
    
    # 执行计算
    result = calculate_net_value(
        address=ADDRESS,
        interval=INTERVAL,
        enable_csv=ENABLE_CSV,
        enable_plot=ENABLE_CHART,
        save_to_db=SAVE_TO_DB,
        plot_dpi=CHART_DPI
    )
    
    if result is not None:
        print("\n✅ 计算完成！")
    else:
        print("\n❌ 计算失败")


if __name__ == '__main__':
    main()

