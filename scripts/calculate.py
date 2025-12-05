# -*- coding: utf-8 -*-
"""
净值计算脚本
===========

从 run_calculate_net_value_v2.py 提取的核心计算逻辑
"""
import sys
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 设置输出编码为UTF-8（解决Windows GBK编码问题）
# 只在标准输出未被替换时设置（避免Web环境中的冲突）
# 检查是否是标准 stdout（不是自定义对象）
if sys.platform == 'win32' and hasattr(sys.stdout, 'buffer') and hasattr(sys.stdout.buffer, 'raw'):
    import io
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', line_buffering=True)
    except (ValueError, AttributeError):
        # 如果已经被重定向或包装过，跳过
        pass

# 设置中文字体（解决中文显示问题）
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

from config import *
from main.caculate_net_value_v2 import NetValueCalculatorV2
from main.net_value_timescale_manager import NetValueTimescaleManager


def calculate_net_value(
    address: str,
    interval: str = DEFAULT_INTERVAL,
    enable_csv: bool = ENABLE_CSV_EXPORT,
    enable_plot: bool = ENABLE_CHART_EXPORT,
    save_to_db: bool = True,
    plot_dpi: int = CHART_DPI,
    incremental: bool = True
):
    """
    计算指定地址的净值
    
    参数:
        address: 账户地址
        interval: 时间区间
        enable_csv: 是否导出CSV
        enable_plot: 是否生成图表
        save_to_db: 是否保存到数据库
        plot_dpi: 图表分辨率
        incremental: 是否增量更新（True: 只追加新数据，False: 全量覆盖）
    """
    print("=" * 80, flush=True)
    print("净值计算 - NetValueCalculatorV2", flush=True)
    print("=" * 80, flush=True)
    print(f"\n配置信息:", flush=True)
    print(f"   账户地址: {address}", flush=True)
    print(f"   数据来源: API", flush=True)
    print(f"   时间区间: {interval}", flush=True)
    print(f"   调试模式: {DEBUG_MODE}", flush=True)
    print(f"   生成CSV文件: {enable_csv}", flush=True)
    print(f"   保存到数据库: {save_to_db}", flush=True)
    print(f"   更新模式: {'增量更新' if incremental else '⚠️ 全量覆盖'}", flush=True)
    
    if save_to_db:
        print(f"   数据库类型: TimescaleDB", flush=True)
        print(f"   数据库地址: {TIMESCALE_CONFIG['host']}:{TIMESCALE_CONFIG['port']}/{TIMESCALE_CONFIG['database']}", flush=True)
    
    try:
        # ==================== 初始化并计算 ====================
        calculator = NetValueCalculatorV2(
            address=address,
            interval=interval,
            debug=DEBUG_MODE
        )
        
        if not calculator.initialize():
            print("\n❌ 初始化失败", flush=True)
            return None
        
        # 计算现货账户价值
        if not calculator.calculate_spot_account_value():
            print("\n❌ 步骤5失败：计算现货账户价值", flush=True)
            return None
        
        print("\n✅ 步骤5完成：现货账户价值计算完成", flush=True)
        
        # 计算合约账户价值
        if not calculator.calculate_perp_account_value():
            print("\n❌ 步骤6失败：计算合约账户价值", flush=True)
            return None
        
        print("\n✅ 步骤6完成：合约账户价值计算完成", flush=True)
        
        # 计算净值
        if not calculator.calculate_net_value():
            print("\n❌ 步骤7失败：计算净值", flush=True)
            return None
        
        print("\n✅ 步骤7完成：净值计算完成", flush=True)
        
        df_result = calculator.intervals_df
        
        # ==================== 导出CSV ====================
        if enable_csv:
            print("\n" + "=" * 80, flush=True)
            print("导出结果到CSV...", flush=True)
            print("=" * 80, flush=True)
            
            csv_path = os.path.join(CSV_OUTPUT_DIR, f"{address[:10]}_NetValue_{interval}.csv")
            
            # 确保时间戳列保持完整精度
            df_export = df_result.copy()
            df_export['timestamp'] = df_export['timestamp'].astype('int64')
            
            # 导出CSV
            df_export.to_csv(csv_path, encoding='utf-8-sig', index=False)
            
            print(f"✅ CSV文件已保存到: {csv_path}", flush=True)
        else:
            print("\n[INFO] 已禁用CSV文件生成（enable_csv=False）", flush=True)
        
        # ==================== 保存到 TimescaleDB ====================
        if save_to_db:
            print("\n" + "=" * 80, flush=True)
            print("保存到 TimescaleDB...", flush=True)
            print("=" * 80, flush=True)
            
            try:
                db_manager = NetValueTimescaleManager(**TIMESCALE_CONFIG)
                
                # 查询保存前的更新记录
                print("\n📊 查询保存前的更新记录...", flush=True)
                old_record = db_manager.get_update_record(address)
                if old_record and old_record.get(interval):
                    old_timestamp = old_record[interval]
                    old_time = datetime.fromtimestamp(old_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"   旧记录 ({interval}): {old_time}", flush=True)
                else:
                    print(f"   旧记录 ({interval}): 无记录（首次更新）", flush=True)
                
                # 保存数据（会自动更新记录表）
                result = db_manager.save_net_value_data(
                    address=address,
                    interval=interval,
                    df=df_result,
                    incremental=incremental
                )
                
                # 保存第一笔交易时间（如果存在）
                # 全量覆盖时也需要更新，因为可能有变化
                first_trade_ts = calculator.get_first_trade_timestamp()
                if first_trade_ts:
                    db_manager.update_first_trade_timestamp(address, first_trade_ts)
                    print(f"   ✅ 第一笔交易时间: {datetime.fromtimestamp(first_trade_ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
                
                # 查询保存后的更新记录
                print("\n📊 查询保存后的更新记录...", flush=True)
                new_record = db_manager.get_update_record(address)
                if new_record and new_record.get(interval):
                    new_timestamp = new_record[interval]
                    new_time = datetime.fromtimestamp(new_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"   新记录 ({interval}): {new_time}", flush=True)
                    
                    # 计算时间差
                    if old_record and old_record.get(interval):
                        time_diff_sec = (new_timestamp - old_timestamp) / 1000
                        time_diff_hours = time_diff_sec / 3600
                        if time_diff_hours < 1:
                            print(f"   时间跨度: {time_diff_sec / 60:.1f} 分钟", flush=True)
                        elif time_diff_hours < 24:
                            print(f"   时间跨度: {time_diff_hours:.1f} 小时", flush=True)
                        else:
                            print(f"   时间跨度: {time_diff_hours / 24:.1f} 天", flush=True)
                
                print("\n" + "=" * 80, flush=True)
                print(f"✅ 数据已保存到TimescaleDB", flush=True)
                print(f"   插入: {result['inserted']} 条", flush=True)
                print(f"   跳过: {result['skipped']} 条", flush=True)
                print(f"   总计: {result['total']} 条", flush=True)
                print("=" * 80, flush=True)
                
                # 显示所有周期的更新状态（可选）
                if new_record:
                    print("\n📋 该地址所有周期的更新状态:", flush=True)
                    print("-" * 80, flush=True)
                    from config.settings import SUPPORTED_INTERVALS
                    for int_name in SUPPORTED_INTERVALS:
                        timestamp = new_record.get(int_name)
                        if timestamp:
                            time_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                            # 计算距离现在的时间
                            import time as time_module
                            current_time = int(time_module.time() * 1000)
                            hours_ago = (current_time - timestamp) / (1000 * 60 * 60)
                            
                            if int_name == interval:
                                status = "⭐ (刚刚更新)"
                            elif hours_ago < 1:
                                status = "🟢 (很新鲜)"
                            elif hours_ago < 24:
                                status = "🟡 (较新)"
                            else:
                                status = f"🔴 ({hours_ago / 24:.1f}天前)"
                            
                            print(f"   {int_name:6s}: {time_str}  {status}", flush=True)
                        else:
                            print(f"   {int_name:6s}: 未更新", flush=True)
                    print("-" * 80, flush=True)
                
            except Exception as e:
                print(f"\n⚠️ 数据库保存失败: {e}", flush=True)
                import traceback
                traceback.print_exc()
        else:
            print("\n[INFO] 已禁用数据库保存（save_to_db=False）", flush=True)
        
        # ==================== 生成图表 ====================
        if enable_plot:
            print("\n" + "=" * 80, flush=True)
            print("生成图表...", flush=True)
            print("=" * 80, flush=True)
            
            _generate_chart(df_result, address, interval, plot_dpi)
        else:
            print("\n[INFO] 已禁用图表生成（enable_plot=False）", flush=True)
        
        print("\n" + "=" * 80, flush=True)
        print("✅ 所有任务完成！", flush=True)
        print("=" * 80, flush=True)
        
        return df_result
        
    except Exception as e:
        try:
            print(f"\n❌ 计算失败: {e}", flush=True)
            import traceback
            traceback.print_exc()
        except (ValueError, OSError):
            # 如果 stdout 已关闭，使用 sys.__stderr__（原始错误输出）
            import sys
            import traceback
            if hasattr(sys, '__stderr__'):
                sys.__stderr__.write(f"\n❌ 计算失败: {e}\n")
                traceback.print_exc(file=sys.__stderr__)
        return None


def _generate_chart(df, address, interval, dpi=150):
    """生成净值图表（内部函数）"""
    # 过滤出有份额的数据
    df_plot = df[abs(df['total_shares']) > 1e-10].copy()
    
    if df_plot.empty:
        print("⚠️ 没有可用于绘图的数据（所有份额为0）", flush=True)
        return
    
    # 找到第一个非零累计PnL
    first_nonzero_pnl_idx = None
    for idx in df_plot.index:
        if abs(df_plot.at[idx, 'cumulative_pnl']) > 1e-6:
            first_nonzero_pnl_idx = idx
            break
    
    if first_nonzero_pnl_idx is not None and PLOT_FROM_FIRST_TRADE:
        df_plot = df_plot.loc[first_nonzero_pnl_idx:].reset_index(drop=True)
        
        # 归一化净值
        if len(df_plot) > 0:
            first_net_value = df_plot.iloc[0]['net_value']
            if abs(first_net_value) > 1e-10:
                df_plot['normalized_net_value'] = df_plot['net_value'] / first_net_value
            else:
                df_plot['normalized_net_value'] = df_plot['net_value']
        else:
            df_plot['normalized_net_value'] = df_plot['net_value']
    else:
        df_plot['normalized_net_value'] = df_plot['net_value']
    
    # 转换时间戳
    df_plot['datetime'] = pd.to_datetime(df_plot['timestamp'], unit='ms')
    
    # 创建图表
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), dpi=dpi)
    fig.suptitle(f'净值分析 - {address[:10]} ({interval})', fontsize=16, fontweight='bold')
    
    # 子图1：归一化净值
    ax1 = axes[0]
    ax1.plot(df_plot['datetime'], df_plot['normalized_net_value'], 
             linewidth=1.5, color='#2E86AB', label='归一化净值')
    ax1.axhline(y=1.0, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)
    ax1.set_xlabel('时间', fontsize=11)
    ax1.set_ylabel('归一化净值', fontsize=11)
    ax1.set_title('归一化净值变化（起始值=1.0）', fontsize=12, pad=10)
    ax1.grid(True, alpha=0.3, linestyle=':', linewidth=0.5)
    ax1.legend(loc='best', fontsize=10)
    
    # 子图2：累计盈亏
    ax2 = axes[1]
    ax2.plot(df_plot['datetime'], df_plot['cumulative_pnl'], 
             linewidth=1.5, color='#A23B72', label='累计盈亏')
    ax2.axhline(y=0, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)
    ax2.set_xlabel('时间', fontsize=11)
    ax2.set_ylabel('累计盈亏 (USD)', fontsize=11)
    ax2.set_title('累计盈亏', fontsize=12, pad=10)
    ax2.grid(True, alpha=0.3, linestyle=':', linewidth=0.5)
    ax2.legend(loc='best', fontsize=10)
    
    # 格式化x轴日期
    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    # 保存图表
    chart_path = os.path.join(CHART_OUTPUT_DIR, f"{address[:10]}_NetValue_{interval}_chart.png")
    plt.savefig(chart_path, dpi=dpi, bbox_inches='tight')
    plt.close()
    
    print(f"✅ 图表已保存到: {chart_path}", flush=True)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='计算账户净值')
    parser.add_argument('--address', required=True, help='账户地址')
    parser.add_argument('--interval', default=DEFAULT_INTERVAL, help='时间区间')
    parser.add_argument('--csv', action='store_true', help='导出CSV')
    parser.add_argument('--chart', action='store_true', help='生成图表')
    parser.add_argument('--no-db', action='store_true', help='不保存到数据库')
    parser.add_argument('--dpi', type=int, default=CHART_DPI, help='图表分辨率')
    
    args = parser.parse_args()
    
    calculate_net_value(
        address=args.address,
        interval=args.interval,
        enable_csv=args.csv,
        enable_plot=args.chart,
        save_to_db=not args.no_db,
        plot_dpi=args.dpi
    )

