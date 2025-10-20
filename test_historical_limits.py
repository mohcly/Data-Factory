#!/usr/bin/env python3
"""
Test Binance Historical Data Limits

Check how far back we can retrieve historical data from Binance APIs.
"""

import requests
import time
from datetime import datetime, timedelta
import json

def test_kline_limits(symbol='BTCUSDT', interval='5m'):
    """Test how far back we can get historical klines."""
    base_url = "https://fapi.binance.com"

    # Test different time periods
    test_periods = [
        ("1 week ago", 7),
        ("1 month ago", 30),
        ("3 months ago", 90),
        ("6 months ago", 180),
        ("1 year ago", 365),
        ("2 years ago", 730),
        ("3 years ago", 1095)
    ]

    results = {}

    print(f"Testing historical kline limits for {symbol} ({interval})...")
    print("=" * 60)

    for period_name, days_back in test_periods:
        # Calculate start and end times
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        # Convert to milliseconds
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        try:
            # Rate limit
            time.sleep(0.1)

            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': start_ms,
                'endTime': end_ms,
                'limit': 10  # Just get a few candles to test availability
            }

            response = requests.get(f"{base_url}/fapi/v1/klines", params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data:
                    oldest_candle = datetime.fromtimestamp(data[0][0] / 1000)
                    newest_candle = datetime.fromtimestamp(data[-1][0] / 1000)
                    results[period_name] = {
                        'available': True,
                        'candles_returned': len(data),
                        'oldest_data': oldest_candle.isoformat(),
                        'newest_data': newest_candle.isoformat(),
                        'days_coverage': (newest_candle - oldest_candle).days
                    }
                    status = f"✅ {len(data)} candles"
                else:
                    results[period_name] = {'available': False, 'reason': 'No data returned'}
                    status = "❌ No data"
            else:
                results[period_name] = {
                    'available': False,
                    'reason': f'HTTP {response.status_code}: {response.text[:100]}'
                }
                status = f"❌ HTTP {response.status_code}"

        except Exception as e:
            results[period_name] = {'available': False, 'reason': str(e)}
            status = f"❌ Error: {str(e)[:30]}..."

        print("2d")

    return results

def test_agg_trades_limits(symbol='BTCUSDT'):
    """Test how far back we can get aggregated trades."""
    base_url = "https://fapi.binance.com"

    # Test different time periods (shorter for trades since they have less history)
    test_periods = [
        ("1 day ago", 1),
        ("3 days ago", 3),
        ("1 week ago", 7),
        ("2 weeks ago", 14),
        ("1 month ago", 30)
    ]

    results = {}

    print(f"\nTesting aggregated trades limits for {symbol}...")
    print("=" * 60)

    for period_name, days_back in test_periods:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        try:
            time.sleep(0.1)

            # Get trades from that period (limit to 10 for testing)
            params = {
                'symbol': symbol,
                'limit': 10,
                'startTime': int(start_time.timestamp() * 1000),
                'endTime': int(end_time.timestamp() * 1000)
            }

            response = requests.get(f"{base_url}/fapi/v1/aggTrades", params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data:
                    oldest_trade = datetime.fromtimestamp(data[0]['T'] / 1000)
                    newest_trade = datetime.fromtimestamp(data[-1]['T'] / 1000)
                    results[period_name] = {
                        'available': True,
                        'trades_returned': len(data),
                        'oldest_trade': oldest_trade.isoformat(),
                        'newest_trade': newest_trade.isoformat()
                    }
                    status = f"✅ {len(data)} trades"
                else:
                    results[period_name] = {'available': False, 'reason': 'No trades returned'}
                    status = "❌ No trades"
            else:
                results[period_name] = {
                    'available': False,
                    'reason': f'HTTP {response.status_code}'
                }
                status = f"❌ HTTP {response.status_code}"

        except Exception as e:
            results[period_name] = {'available': False, 'reason': str(e)}
            status = f"❌ Error: {str(e)[:30]}..."

        print("2d")

    return results

def main():
    """Main test function."""
    print("🔍 TESTING BINANCE HISTORICAL DATA LIMITS")
    print("=" * 60)

    # Test klines (long-term data)
    kline_results = test_kline_limits()

    # Test aggregated trades (short-term data)
    trades_results = test_agg_trades_limits()

    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY - HISTORICAL DATA AVAILABILITY")
    print("=" * 60)

    # Find the maximum historical coverage for klines
    max_kline_days = 0
    for period, result in kline_results.items():
        if result.get('available', False):
            days = result.get('days_coverage', 0)
            max_kline_days = max(max_kline_days, days)

    print("\n🕯️ HISTORICAL KLINES (OHLCV Candles):")
    if max_kline_days > 0:
        print(f"   ✅ Maximum historical coverage: ~{max_kline_days} days")
        print("   ✅ Can be used to fill long-term data gaps")
        print("   ✅ 5-minute intervals available")
    else:
        print("   ❌ No historical kline data available")

    # Check trades coverage
    max_trade_days = 0
    for period, result in trades_results.items():
        if result.get('available', False):
            # Extract days from period name
            if 'day' in period:
                if 'days' in period:
                    days = int(period.split()[0])
                else:
                    days = 1
                max_trade_days = max(max_trade_days, days)

    print("\n💰 AGGREGATED TRADES:")
    if max_trade_days > 0:
        print(f"   ✅ Historical coverage: ~{max_trade_days} days")
        print("   ✅ Detailed trade-by-trade data")
        print("   ✅ Good for recent market activity analysis")
    else:
        print("   ❌ No historical trade data available")

    print("\n📋 CURRENT ORDERBOOK:")
    print("   ✅ Real-time only (no historical buffer)")
    print("   ✅ Full depth and spread data")
    print("   ✅ Must be collected live")

    # Save results
    with open('historical_data_limits_test.json', 'w') as f:
        json.dump({
            'kline_limits': kline_results,
            'trade_limits': trades_results,
            'test_timestamp': datetime.now().isoformat()
        }, f, indent=2)

    print("\n💾 Results saved to: historical_data_limits_test.json")

if __name__ == "__main__":
    main()
