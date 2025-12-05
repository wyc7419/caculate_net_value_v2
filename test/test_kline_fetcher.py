#!/usr/bin/env python3
"""
kline_fetcher 使用示例
展示如何在代码中直接调用API函数

包含以下示例：
1. 使用auto自动选择最佳精度（不显示调试信息）
2. 启用调试信息（显示详细查询过程）
3. 小币种查询（演示自动切换时间周期）
4. 查询时间范围内的价格

debug参数说明：
- debug=False（默认）：不显示调试信息，只返回结果
- debug=True：显示详细的调试信息，包括：
  * 自动选择的时间周期
  * 周期切换警告（如果需要）
  * 一行格式的查询详情（币对、类型、时间、价格、时间差）
"""

import sys
import os

# 添加模块路径
script_dir = os.path.dirname(os.path.abspath(__file__))
main_dir = os.path.join(os.path.dirname(script_dir), 'main')
sys.path.insert(0, main_dir)

from kline_fetcher import get_open_price, get_open_prices  # type: ignore


def example_1_single_timestamp():
    """示例1: 使用auto自动选择最佳精度（不显示调试信息）"""
    print("\n" + "="*60)
    print("示例1: 使用auto自动选择最佳精度（不显示调试信息）")
    print("="*60)
    
    # 使用auto自动选择最合适的时间周期
    coin = "ETH"
    coin_type = "PERP"
    result = get_open_price(
        coin=coin,
        coin_type=coin_type,
        interval="auto",  # 自动选择
        timestamp=1762138800000,
        debug=False  # 不显示调试信息（默认）
    )
    
    if result:
        print(f"\n查询结果:")
        print(f"  时间: {result['time']}")
        print(f"  开盘价: {result['open']}")
        print(f"  时间差: {result['time_diff_ms']}毫秒")
        print(f"  使用周期: {result['interval']}")
    else:
        print(f"\n查询失败: {coin} ({coin_type})")


def example_2_with_debug():
    """示例2: 启用调试信息"""
    print("\n" + "="*60)
    print("示例2: 启用调试信息（显示详细查询过程）")
    print("="*60)
    
    # 启用debug参数，显示详细的查询过程
    # 使用一个确定存在的币种来演示调试功能
    coin = "UBTC"
    coin_type = "spot"
    result = get_open_price(
        coin=coin,
        coin_type=coin_type,
        interval="auto",  # 自动选择
        timestamp=1762138800000,
        debug=True  # 启用调试信息
    )
    
    if result:
        print(f"\n查询结果:")
        print(f"  时间: {result['time']}")
        print(f"  开盘价: {result['open']}")
        print(f"  时间差: {result['time_diff_ms']}毫秒")
        print(f"  使用周期: {result['interval']}")
    else:
        print(f"\n查询失败: {coin} ({coin_type})")
    
    # 注意：预测市场代币（如 XYZ:100）可能不支持 K线查询
    # 或需要使用特殊的 API 端点
    print("\n" + "="*60)
    print("注意事项：")
    print("  - 预测市场代币（如 XYZ:100）可能不支持标准的 K线查询")
    print("  - 如需查询特定代币，请运行 list_all_coins.py 查看所有可用币种")
    print("  - 或者使用 debug=True 参数查看 API 返回的可用币种列表")
    print("="*60)


def example_3_small_coin_debug():
    """示例3: 小币种查询（演示自动切换时间周期）"""
    print("\n" + "="*60)
    print("示例3: 小币种查询（演示自动切换时间周期）")
    print("="*60)
    
    # 对于交易量小的币种，debug=True可以看到周期切换过程
    coin = "HYPE"
    coin_type = "SPOT"
    result = get_open_price(
        coin=coin,
        coin_type=coin_type,
        interval="auto",
        timestamp=1762138800000,
        debug=True  # 启用调试信息，可以看到周期切换
    )
    
    if result:
        print(f"\n查询结果:")
        print(f"  时间: {result['time']}")
        print(f"  开盘价: {result['open']}")
        print(f"  时间差: {result['time_diff_ms']}毫秒")
        print(f"  使用周期: {result['interval']}")
    else:
        print(f"\n查询失败: {coin} ({coin_type})")


def example_4_time_range():
    """示例4: 查询时间范围内的价格"""
    print("\n" + "="*60)
    print("示例4: 查询时间范围内的价格")
    print("="*60)
    
    # 查询2025-11-10 00:00:00 到 2025-11-10 05:00:00（约5小时）
    coin = "@107"
    coin_type = "SPOT"
    results = get_open_prices(
        coin=coin,
        coin_type=coin_type,
        interval="1d",
        start_time=1761494400000,
        end_time=1762963200000  
    )
    
    if results:
        print(f"\n查询到 {len(results)} 条数据:\n")
        for r in results:
            print(f"  {r['time']}: {r['open']}")
    else:
        print(f"\n查询失败: {coin} ({coin_type})")


def example_5_find_prediction_markets():
    """示例5: 查找预测市场代币"""
    print("\n" + "="*60)
    print("示例5: 查找预测市场代币")
    print("="*60)
    
    try:
        from hyperliquid.info import Info
        
        info = Info(skip_ws=True)
        
        if hasattr(info, 'name_to_coin'):
            all_coins = list(info.name_to_coin.keys())
            
            # 查找可能是预测市场的代币
            # 预测市场通常包含数字、特殊字符等
            print("\n查找包含数字的币种（可能是预测市场）：")
            print("-" * 60)
            
            number_coins = [coin for coin in all_coins if any(c.isdigit() for c in coin)]
            
            if number_coins:
                print(f"找到 {len(number_coins)} 个包含数字的币种：\n")
                for coin in sorted(number_coins)[:20]:
                    print(f"  - {coin}")
                if len(number_coins) > 20:
                    print(f"\n  ... 还有 {len(number_coins) - 20} 个")
            else:
                print("  未找到包含数字的币种")
            
            # 测试第一个包含数字的币种
            if number_coins:
                test_coin = number_coins[0]
                print(f"\n尝试查询第一个币种: {test_coin}")
                print("-" * 60)
                
                result = get_open_price(
                    coin=test_coin,
                    coin_type="perp",
                    interval="auto",
                    timestamp=1762138800000,
                    debug=True
                )
                
                if result:
                    print(f"\n✓ 查询成功!")
                    print(f"  开盘价: {result['open']}")
                else:
                    print(f"\n✗ 查询失败")
                    print(f"  提示: 该代币可能不支持 K线查询")
        else:
            print("当前 SDK 版本不支持查看可用币种列表")
            
    except Exception as e:
        print(f"错误: {e}")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("kline_fetcher API 使用示例")
    print("="*60)
    
    # 运行所有示例
    try:
        example_1_single_timestamp()
        example_2_with_debug()
        example_3_small_coin_debug()
        example_4_time_range()
        example_5_find_prediction_markets()
        
        print("\n" + "="*60)
        print("所有示例执行完成!")
        print("="*60 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()

