#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
清空数据库 - 简单入口
====================

直接修改下面的参数，然后运行：
    python clean_db.py

无需命令行参数！
"""
import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ==================== 在这里修改参数 ====================

# 指定要清空的时间区间（留空则清空所有）
# 可选：'1h', '2h', '4h', '8h', '12h', '1d'
# 示例：INTERVAL = '1h' 或 INTERVAL = None
INTERVAL = None  # None = 清空所有，或指定如 '1h'

# =======================================================


def main():
    """清空数据库"""
    print("\n" + "="*60)
    print("清空 TimescaleDB 数据库")
    print("="*60)
    
    if INTERVAL:
        print(f"\n目标：清空时间区间 {INTERVAL}")
    else:
        print(f"\n目标：清空所有时间区间")
    
    print("\n" + "="*60 + "\n")
    
    # 导入清理函数
    from scripts.clean_database import clean_all_data, clean_interval
    
    # 执行清理
    if INTERVAL:
        clean_interval(INTERVAL)
    else:
        clean_all_data()


if __name__ == '__main__':
    main()

