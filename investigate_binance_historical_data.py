#!/usr/bin/env python3
"""
Investigate Binance Historical Orderbook Data Availability

This script tests various Binance API endpoints to determine what historical
orderbook data is available for data recovery purposes.

Author: MVP Crypto Data Factory
Created: 2025-10-19
"""

import requests
import time
import json
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional

class BinanceHistoricalDataInvestigator:
    """Investigate available historical data from Binance APIs."""

    def __init__(self):
        self.base_url = "https://fapi.binance.com"
        self.test_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

    def test_current_depth_snapshot(self, symbol: str) -> Dict:
        """Test current orderbook depth snapshot API."""
        try:
            url = f"{self.base_url}/fapi/v1/depth"
            params = {'symbol': symbol, 'limit': 100}
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return {
                    'available': True,
                    'bids_count': len(data.get('bids', [])),
                    'asks_count': len(data.get('asks', [])),
                    'last_update_id': data.get('lastUpdateId'),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {'available': False, 'error': f'HTTP {response.status_code}', 'response': response.text[:200]}

        except Exception as e:
            return {'available': False, 'error': str(e)}

    def test_historical_klines(self, symbol: str, interval: str = '5m', limit: int = 100) -> Dict:
        """Test historical klines (candles) API."""
        try:
            url = f"{self.base_url}/fapi/v1/klines"
            # Get last 24 hours of 5-minute candles
            end_time = int(time.time() * 1000)
            start_time = end_time - (24 * 60 * 60 * 1000)  # 24 hours ago

            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': start_time,
                'endTime': end_time,
                'limit': limit
            }

            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return {
                    'available': True,
                    'candles_count': len(data),
                    'oldest_timestamp': datetime.fromtimestamp(data[0][0]/1000).isoformat() if data else None,
                    'newest_timestamp': datetime.fromtimestamp(data[-1][0]/1000).isoformat() if data else None,
                    'sample_candle': data[-1] if data else None
                }
            else:
                return {'available': False, 'error': f'HTTP {response.status_code}'}

        except Exception as e:
            return {'available': False, 'error': str(e)}

    def test_agg_trades(self, symbol: str, limit: int = 100) -> Dict:
        """Test aggregated trades API for historical trade data."""
        try:
            url = f"{self.base_url}/fapi/v1/aggTrades"
            # Get recent aggregated trades
            params = {
                'symbol': symbol,
                'limit': limit
            }

            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data:
                    timestamps = [datetime.fromtimestamp(trade['T']/1000) for trade in data]
                    return {
                        'available': True,
                        'trades_count': len(data),
                        'oldest_trade': min(timestamps).isoformat(),
                        'newest_trade': max(timestamps).isoformat(),
                        'price_range': f"{min(float(t['p']) for t in data):.2f} - {max(float(t['p']) for t in data):.2f}",
                        'volume_range': f"{min(float(t['q']) for t in data):.2f} - {max(float(t['q']) for t in data):.2f}"
                    }
                else:
                    return {'available': True, 'trades_count': 0}
            else:
                return {'available': False, 'error': f'HTTP {response.status_code}'}

        except Exception as e:
            return {'available': False, 'error': str(e)}

    def check_rate_limits(self) -> Dict:
        """Check current API rate limit status."""
        try:
            # This endpoint shows rate limit information
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                rate_limits = data.get('rateLimits', [])

                futures_limits = [limit for limit in rate_limits if limit.get('rateLimitType') == 'REQUEST_WEIGHT']

                return {
                    'available': True,
                    'futures_limits': futures_limits,
                    'raw_response': data
                }
            else:
                return {'available': False, 'error': f'HTTP {response.status_code}'}

        except Exception as e:
            return {'available': False, 'error': str(e)}

    def investigate_all_options(self) -> Dict:
        """Comprehensive investigation of all available historical data options."""
        results = {
            'investigation_timestamp': datetime.now().isoformat(),
            'summary': {},
            'detailed_results': {}
        }

        print("🔍 Investigating Binance Historical Data Options...")
        print("=" * 60)

        # 1. Current Depth Snapshots
        print("1️⃣ Testing Current Orderbook Depth Snapshots...")
        depth_results = {}
        for symbol in self.test_symbols:
            result = self.test_current_depth_snapshot(symbol)
            depth_results[symbol] = result
            status = "✅ Available" if result['available'] else f"❌ {result.get('error', 'Unknown error')}"
            print(f"   {symbol}: {status}")
            if result['available']:
                print(f"      Bids: {result['bids_count']}, Asks: {result['asks_count']}")

        results['detailed_results']['depth_snapshots'] = depth_results

        # 2. Historical Klines (Candles)
        print("\n2️⃣ Testing Historical Klines (OHLCV Candles)...")
        klines_results = {}
        for symbol in self.test_symbols:
            result = self.test_historical_klines(symbol, '5m', 100)
            klines_results[symbol] = result
            status = "✅ Available" if result['available'] else f"❌ {result.get('error', 'Unknown error')}"
            print(f"   {symbol}: {status}")
            if result['available']:
                print(f"      Candles: {result['candles_count']}, Time range: {result['oldest_timestamp']} to {result['newest_timestamp']}")

        results['detailed_results']['historical_klines'] = klines_results

        # 3. Aggregated Trades
        print("\n3️⃣ Testing Historical Aggregated Trades...")
        trades_results = {}
        for symbol in self.test_symbols:
            result = self.test_agg_trades(symbol, 100)
            trades_results[symbol] = result
            status = "✅ Available" if result['available'] else f"❌ {result.get('error', 'Unknown error')}"
            print(f"   {symbol}: {status}")
            if result['available'] and result['trades_count'] > 0:
                print(f"      Trades: {result['trades_count']}, Price range: {result['price_range']}")

        results['detailed_results']['aggregated_trades'] = trades_results

        # 4. Rate Limits
        print("\n4️⃣ Checking API Rate Limits...")
        rate_limit_info = self.check_rate_limits()
        if rate_limit_info['available']:
            print("   ✅ Rate limit info retrieved")
            for limit in rate_limit_info.get('futures_limits', []):
                print(f"      {limit.get('interval')}: {limit.get('limit')} requests")
        else:
            print(f"   ❌ Rate limit check failed: {rate_limit_info.get('error')}")

        results['detailed_results']['rate_limits'] = rate_limit_info

        # Summary
        print("\n" + "=" * 60)
        print("📊 SUMMARY - Data Recovery Options:")
        print("=" * 60)

        # Current snapshots
        depth_available = any(r['available'] for r in depth_results.values())
        results['summary']['current_depth_snapshots'] = {
            'available': depth_available,
            'description': 'Real-time orderbook snapshots (not historical)',
            'usefulness': 'Limited - only current state, no historical data'
        }

        # Historical klines
        klines_available = any(r['available'] for r in klines_results.values())
        results['summary']['historical_klines'] = {
            'available': klines_available,
            'description': 'OHLCV candle data (5m, 15m, 1h, etc.)',
            'usefulness': 'Very useful - can fill gaps with price action data',
            'time_depth': '~1-2 years of historical data'
        }

        # Aggregated trades
        trades_available = any(r['available'] for r in trades_results.values())
        results['summary']['aggregated_trades'] = {
            'available': trades_available,
            'description': 'Historical trade data with timestamps',
            'usefulness': 'Useful - shows actual executed trades',
            'time_depth': '~1-2 days of recent trade history'
        }

        for option, details in results['summary'].items():
            status = "✅ AVAILABLE" if details['available'] else "❌ NOT AVAILABLE"
            print(f"{status}: {option.replace('_', ' ').title()}")
            print(f"   {details['description']}")
            print(f"   Usefulness: {details['usefulness']}")
            if 'time_depth' in details:
                print(f"   Historical depth: {details['time_depth']}")
            print()

        return results

def main():
    """Main investigation function."""
    investigator = BinanceHistoricalDataInvestigator()
    results = investigator.investigate_all_options()

    # Save results to file
    with open('binance_historical_data_investigation.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print("💾 Results saved to: binance_historical_data_investigation.json")

    # Provide recommendations
    print("\n🎯 RECOMMENDATIONS FOR DATA RECOVERY:")
    print("=" * 50)

    if results['summary']['historical_klines']['available']:
        print("✅ BEST OPTION: Use Historical Klines API")
        print("   • Available: 5m, 15m, 1h, 4h, 1d candles")
        print("   • Historical depth: ~1-2 years")
        print("   • Can fill price action gaps in your orderbook data")
        print("   • Much more reliable than real-time orderbook streams")

    if results['summary']['aggregated_trades']['available']:
        print("\n✅ SECONDARY OPTION: Use Aggregated Trades API")
        print("   • Shows actual executed trades with timestamps")
        print("   • Historical depth: ~1-2 days")
        print("   • Good for understanding market activity during gaps")

    if results['summary']['current_depth_snapshots']['available']:
        print("\n⚠️ LIMITED OPTION: Current Depth Snapshots")
        print("   • Only provides current market state")
        print("   • Cannot fill historical gaps")
        print("   • Useful for resuming monitoring, not recovery")

    print("\n🚀 NEXT STEPS:")
    print("1. Implement historical klines data recovery")
    print("2. Use during monitoring gaps to maintain continuity")
    print("3. Consider as backup data source for future gaps")

if __name__ == "__main__":
    main()
