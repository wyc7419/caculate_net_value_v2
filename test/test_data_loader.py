# -*- coding: utf-8 -*-
"""
DataLoader 使用示例（更新版）
============================

演示如何使用最新版本的 DataLoader：
1. 从 API 加载交易数据
2. 自动推断 type 字段
3. 使用配置文件管理设置
4. 从数据库加载其他数据（资金费、账本、快照）

更新日期：2025-12-02
版本：v2.0（支持 API + type 推断）
"""

import sys
import os

# 添加模块路径
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from main.data_loader import DataLoader


def example_1_load_trades_from_api():
    """示例1：从 API 加载交易数据（新功能）⭐"""
    print("\n" + "="*80)
    print("示例1：从 API 加载交易数据（新功能）")
    print("="*80)
    
    # 创建数据加载器（API 配置自动从 config/api.py 加载）
    loader = DataLoader()
    
    # 指定账户地址
    address = "0x0000000afcd4de376f2bf0094cdd01712f125995"
    
    print(f"\n正在从 API 加载交易数据...")
    print(f"地址: {address}")
    
    # 加载所有交易数据（从 API）
    trades = loader.load_trades(address)
    
    print(f"\n✅ 成功从 API 加载交易数据:")
    print(f"   - 总交易数: {len(trades)} 条")
    
    # 统计交易类型（展示 type 自动推断功能）
    perp_trades = [t for t in trades if t.get('type') == 'perp']
    spot_trades = [t for t in trades if t.get('type') == 'spot']
    
    print(f"\n交易类型分布（自动推断）:")
    print(f"   - 合约交易(perp): {len(perp_trades)} 条")
    print(f"   - 现货交易(spot): {len(spot_trades)} 条")
    
    # 显示前3条交易的详情
    if trades:
        print(f"\n前3条交易详情:")
        for i, trade in enumerate(trades[:3], 1):
            coin = trade.get('coin', 'N/A')
            dir_val = trade.get('dir', 'N/A')
            type_val = trade.get('type', 'N/A')
            side = trade.get('side', 'N/A')
            print(f"   {i}. {coin:10s} | dir: {dir_val:20s} | type: {type_val:4s} | side: {side}")


def example_2_test_type_inference():
    """示例2：测试 type 字段自动推断（新功能）⭐"""
    print("\n" + "="*80)
    print("示例2：测试 type 字段自动推断")
    print("="*80)
    
    loader = DataLoader()
    address = "0x0000000afcd4de376f2bf0094cdd01712f125995"
    
    print(f"\n正在加载交易数据并测试 type 推断...")
    trades = loader.load_trades(address)
    
    # 统计各种 dir 值及其对应的 type
    dir_type_mapping = {}
    for trade in trades:
        dir_val = trade.get('dir', '(空)')
        type_val = trade.get('type', '(未知)')
        
        if dir_val not in dir_type_mapping:
            dir_type_mapping[dir_val] = {'type': type_val, 'count': 0}
        dir_type_mapping[dir_val]['count'] += 1
    
    print(f"\n✅ dir 值与 type 推断结果:")
    print(f"{'dir 值':<30s} | {'推断 type':^10s} | {'数量':>6s}")
    print("-" * 52)
    
    for dir_val, info in sorted(dir_type_mapping.items(), key=lambda x: x[1]['count'], reverse=True):
        type_val = info['type']
        count = info['count']
        type_display = f"→ {type_val}" 
        print(f"{dir_val:<30s} | {type_display:^10s} | {count:>6d}")
    
    # 验证规则
    perp_count = sum(1 for t in trades if t.get('type') == 'perp')
    spot_count = sum(1 for t in trades if t.get('type') == 'spot')
    
    print(f"\n汇总:")
    print(f"   - 合约交易(perp): {perp_count} 条 ({perp_count/len(trades)*100:.1f}%)")
    print(f"   - 现货交易(spot): {spot_count} 条 ({spot_count/len(trades)*100:.1f}%)")


def example_3_load_time_range():
    """示例3：加载指定时间范围的交易数据（新功能）⭐"""
    print("\n" + "="*80)
    print("示例3：加载指定时间范围的交易数据")
    print("="*80)
    
    loader = DataLoader()
    address = "0x0000000afcd4de376f2bf0094cdd01712f125995"
    
    # 加载所有交易（用于对比）
    print("\n[1] 加载所有交易数据...")
    all_trades = loader.load_trades(address, range_type="All")
    print(f"    ✅ 加载了 {len(all_trades)} 笔交易")
    
    # 加载指定时间范围的交易（示例：2025年9月）
    print("\n[2] 加载2025年9月的交易数据...")
    start_time = "2025-09-01T00:00:00Z"
    end_time = "2025-10-01T00:00:00Z"
    sept_trades = loader.load_trades(
        address,
        start_time=start_time,
        end_time=end_time,
        range_type="Select"
    )
    print(f"    ✅ 加载了 {len(sept_trades)} 笔交易")
    print(f"    时间范围: {start_time} ~ {end_time}")
    
    if sept_trades:
        # 统计9月份的交易类型
        perp = sum(1 for t in sept_trades if t.get('type') == 'perp')
        spot = sum(1 for t in sept_trades if t.get('type') == 'spot')
        print(f"\n    9月份交易分布:")
        print(f"       - 合约交易: {perp} 条")
        print(f"       - 现货交易: {spot} 条")


def example_4_config_usage():
    """示例4：配置文件使用（新功能）⭐"""
    print("\n" + "="*80)
    print("示例4：配置文件使用")
    print("="*80)
    
    print("\n✅ DataLoader 现在自动从配置文件加载设置:")
    print("   - API 配置: config/api.py")
    print("   - 数据库配置: config/database.py")
    print("   - 通用配置: config/settings.py")
    
    # 创建 DataLoader（无需传递参数）
    loader = DataLoader()
    
    print(f"\n当前配置:")
    print(f"   - API 基础 URL: {loader.api_base_url}")
    print(f"   - API 超时: {loader.api_timeout} 秒")
    print(f"   - 数据源类型: {loader.data_source_type}")
    
    # 展示如何通过环境变量修改配置
    print(f"\n💡 提示：可通过环境变量修改配置:")
    print(f"   export API_BASE_URL=http://your-api-server.com:8000")
    print(f"   export API_TIMEOUT=60")
    print(f"   或在 .env 文件中设置")


def example_5_integrate_with_class():
    """示例5：在类中集成 DataLoader（更新版）"""
    print("\n" + "="*80)
    print("示例5：在类中集成 DataLoader")
    print("="*80)
    
    class TradeAnalyzer:
        """交易分析器示例类（使用新版 DataLoader）"""
        
        def __init__(self, address: str):
            self.address = address
            self.loader = DataLoader()  # 配置自动加载
            self.trades = []
        
        def load_trades(self):
            """加载交易数据（从 API）"""
            print(f"\n[TradeAnalyzer] 从 API 加载交易数据...")
            print(f"   地址: {self.address}")
            
            self.trades = self.loader.load_trades(self.address)
            
            print(f"[TradeAnalyzer] ✅ 加载完成: {len(self.trades)} 条交易")
            return True
        
        def analyze_by_type(self):
            """按类型分析交易"""
            if not self.trades:
                print("[TradeAnalyzer] ⚠️  警告：未加载数据")
                return
            
            perp_trades = [t for t in self.trades if t.get('type') == 'perp']
            spot_trades = [t for t in self.trades if t.get('type') == 'spot']
            
            print(f"\n[TradeAnalyzer] 分析结果:")
            print(f"   - 合约交易: {len(perp_trades)} 条")
            print(f"   - 现货交易: {len(spot_trades)} 条")
            
            # 统计合约交易的 dir 分布
            if perp_trades:
                perp_dirs = {}
                for t in perp_trades:
                    dir_val = t.get('dir', 'N/A')
                    perp_dirs[dir_val] = perp_dirs.get(dir_val, 0) + 1
                
                print(f"\n   合约交易类型分布（top 3）:")
                for dir_val, count in sorted(perp_dirs.items(), key=lambda x: x[1], reverse=True)[:3]:
                    print(f"      - {dir_val}: {count} 条")
    
    # 使用示例类
    address = "0x0000000afcd4de376f2bf0094cdd01712f125995"
    analyzer = TradeAnalyzer(address)
    analyzer.load_trades()
    analyzer.analyze_by_type()


def main():
    """运行所有示例"""
    print("="*80)
    print("DataLoader 使用示例集合（v2.0）")
    print("="*80)
    print("\n✨ 新功能:")
    print("   1. 从 API 加载交易数据")
    print("   2. 自动推断 type 字段（perp/spot）")
    print("   3. 支持时间范围查询")
    print("   4. 配置文件管理")
    print("   5. 未知 dir 值警告")
    
    try:
        # 示例1: 从 API 加载交易数据
        example_1_load_trades_from_api()
        
        # 示例2: 测试 type 字段推断
        example_2_test_type_inference()
        
        # 示例3: 加载指定时间范围
        example_3_load_time_range()
        
        # 示例4: 配置文件使用
        example_4_config_usage()
        
        # 示例5: 在类中集成
        example_5_integrate_with_class()
        
        print("\n" + "="*80)
        print("✅ 所有示例运行完成！")
        print("="*80)
        print("\n📚 更多信息:")
        print("   - API 文档: docs/DATA_LOADER_API.md")
        print("   - Type 推断规则: TYPE_INFERENCE_RULES.md")
        print("   - 配置说明: docs/CONFIG_STRUCTURE.md")
        
    except Exception as e:
        print(f"\n❌ [ERROR] 错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

