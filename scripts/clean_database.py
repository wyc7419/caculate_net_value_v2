# -*- coding: utf-8 -*-
"""
清空 TimescaleDB 数据库脚本
==========================

功能：
1. 列出所有净值数据表
2. 删除所有表中的数据
3. 或删除整个表（包括结构）

使用方法：
    python -m scripts.clean_database
    或
    python scripts/clean_database.py
"""

import psycopg2
from psycopg2 import sql
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import TIMESCALE_CONFIG


def get_connection():
    """获取数据库连接"""
    return psycopg2.connect(**TIMESCALE_CONFIG)


def list_net_value_tables():
    """列出所有净值数据表"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'net_value_%'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    finally:
        cursor.close()
        conn.close()


def get_table_row_count(table_name: str) -> int:
    """获取表的行数"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
            sql.Identifier(table_name)
        ))
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        print(f"[ERROR] 获取表 {table_name} 行数失败: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()


def truncate_table(table_name: str):
    """清空表数据（保留表结构）"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(
            sql.Identifier(table_name)
        ))
        conn.commit()
        print(f"✅ 表 {table_name} 已清空")
    except Exception as e:
        conn.rollback()
        print(f"❌ 清空表 {table_name} 失败: {e}")
    finally:
        cursor.close()
        conn.close()


def drop_table(table_name: str):
    """删除表（包括表结构）"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
            sql.Identifier(table_name)
        ))
        conn.commit()
        print(f"✅ 表 {table_name} 已删除")
    except Exception as e:
        conn.rollback()
        print(f"❌ 删除表 {table_name} 失败: {e}")
    finally:
        cursor.close()
        conn.close()


def clean_update_records_table():
    """清空更新记录表"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("TRUNCATE TABLE net_value_update_records CASCADE")
        conn.commit()
        print(f"✅ 更新记录表已清空")
    except Exception as e:
        conn.rollback()
        print(f"⚠️  清空更新记录表失败（可能不存在）: {e}")
    finally:
        cursor.close()
        conn.close()


def clean_all_data():
    """清空所有净值数据表的数据"""
    print("\n" + "="*60)
    print("清空 TimescaleDB 数据库")
    print("="*60 + "\n")
    
    # 连接测试
    try:
        conn = get_connection()
        print(f"✅ 成功连接到数据库: {TIMESCALE_CONFIG['host']}")
        conn.close()
    except Exception as e:
        print(f"❌ 无法连接到数据库: {e}")
        return
    
    # 列出所有表
    print("\n查找净值数据表...")
    tables = list_net_value_tables()
    
    if not tables:
        print("\n没有找到净值数据表")
        return
    
    print(f"\n找到 {len(tables)} 个净值数据表:\n")
    
    # 显示表信息
    total_rows = 0
    for idx, table_name in enumerate(tables, 1):
        row_count = get_table_row_count(table_name)
        total_rows += row_count
        print(f"  {idx}. {table_name:20s} - {row_count:,} 条记录")
    
    print(f"\n总计: {total_rows:,} 条记录")
    
    # 确认操作
    print("\n" + "="*60)
    print("⚠️  警告：此操作将清空所有表中的数据！")
    print("="*60)
    
    choice = input("\n请选择操作:\n  1) 清空数据(保留表结构)\n  2) 删除表(包括结构)\n  3) 取消\n\n请输入选项 (1/2/3): ")
    
    if choice == '1':
        print("\n开始清空数据...\n")
        for table_name in tables:
            truncate_table(table_name)
        
        # 同时清空更新记录表
        print("")
        clean_update_records_table()
        
        print("\n✅ 所有表数据已清空！")
    
    elif choice == '2':
        confirm = input("\n⚠️  确认要删除所有表吗？此操作不可恢复！(yes/no): ")
        if confirm.lower() == 'yes':
            print("\n开始删除表...\n")
            for table_name in tables:
                drop_table(table_name)
            
            # 同时清空更新记录表
            print("")
            clean_update_records_table()
            
            print("\n✅ 所有表已删除！")
        else:
            print("\n❌ 操作已取消")
    
    else:
        print("\n❌ 操作已取消")


def clean_interval(interval: str):
    """清空指定时间区间的数据"""
    table_name = f"net_value_{interval}"
    
    print(f"\n清空表: {table_name}")
    
    try:
        row_count = get_table_row_count(table_name)
        print(f"当前记录数: {row_count:,}")
        
        if row_count == 0:
            print("表中没有数据")
            return
        
        choice = input(f"\n确认清空表 {table_name} 吗？(yes/no): ")
        
        if choice.lower() == 'yes':
            truncate_table(table_name)
            
            # 同时清空更新记录表中对应区间的字段
            print("\n清空更新记录表中的对应字段...")
            conn = get_connection()
            cursor = conn.cursor()
            try:
                column_name = f"time_{interval}"
                cursor.execute(f"""
                    UPDATE net_value_update_records
                    SET {column_name} = NULL
                """)
                conn.commit()
                print(f"✅ 更新记录表中的 {column_name} 字段已清空")
            except Exception as e:
                conn.rollback()
                print(f"⚠️  清空更新记录失败（可能不存在）: {e}")
            finally:
                cursor.close()
                conn.close()
            
            print(f"\n✅ 表 {table_name} 已清空！")
        else:
            print("\n❌ 操作已取消")
    
    except Exception as e:
        print(f"\n❌ 操作失败: {e}")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='清空TimescaleDB数据库')
    parser.add_argument('--interval', help='只清空指定时间区间的表（如: 1h, 1d）')
    
    args = parser.parse_args()
    
    if args.interval:
        clean_interval(args.interval)
    else:
        clean_all_data()

