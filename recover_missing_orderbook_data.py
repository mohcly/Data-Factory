#!/usr/bin/env python3
"""
Recover Missing Orderbook Data using Binance Historical APIs

This script fills gaps in orderbook monitoring by fetching historical klines
and trade data from Binance to maintain data continuity.

Strategy:
1. Identify gaps in existing 5-minute candle data
2. Fetch historical 5-minute klines from Binance API
3. Create synthetic 5-minute candles for missing periods
4. Append to existing orderbook candle files

Author: MVP Crypto Data Factory
Created: 2025-10-19
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_recovery.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OrderbookDataRecovery:
    """Recover missing orderbook data using Binance historical APIs."""

    def __init__(self, orderbook_dir: str = "data/orderbook_5min_candles"):
        self.orderbook_dir = Path(orderbook_dir)
        self.binance_base_url = "https://fapi.binance.com"
        self.quality_symbols = [
            'AAVE', 'ADA', 'AVAX', 'BCH', 'BNB', 'BTC', 'DOGE', 'DOT', 'ETC', 'ETH',
            'FIL', 'HBAR', 'LINK', 'LTC', 'SOL', 'THETA', 'TRX', 'UNI', 'USDC', 'XLM',
            'XMR', 'XRP', 'ZEC'
        ]

        # Rate limiting: Binance allows 2400 requests per minute
        self.request_delay = 0.025  # ~40 requests/second to stay under limit
        self.last_request_time = 0

    def rate_limit_wait(self):
        """Respect Binance API rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()

    def fetch_historical_klines(self, symbol: str, interval: str = '5m',
                               start_time: datetime = None, end_time: datetime = None,
                               limit: int = 500) -> List[List]:
        """
        Fetch historical klines (candles) from Binance.

        Returns list of kline data:
        [
            [
                1499040000000,      // Open time
                "0.01634790",       // Open
                "0.80000000",       // High
                "0.01575800",       // Low
                "0.01577100",       // Close
                "148976.11427815",  // Volume
                1499644799999,      // Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "17928899.62484339" // Unused field, ignore
            ]
        ]
        """
        self.rate_limit_wait()

        try:
            url = f"{self.binance_base_url}/fapi/v1/klines"

            params = {
                'symbol': f"{symbol}USDT",
                'interval': interval,
                'limit': limit
            }

            if start_time:
                params['startTime'] = int(start_time.timestamp() * 1000)
            if end_time:
                params['endTime'] = int(end_time.timestamp() * 1000)

            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch klines for {symbol}: HTTP {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
            return []

    def kline_to_candle(self, kline: List, symbol: str) -> Dict:
        """Convert Binance kline format to our 5-minute candle format."""
        try:
            open_time = datetime.fromtimestamp(kline[0] / 1000)
            close_time = datetime.fromtimestamp(kline[6] / 1000)

            # Round to 5-minute boundary (floor to nearest 5 minutes)
            rounded_time = open_time.replace(second=0, microsecond=0)
            minutes = rounded_time.minute
            rounded_minutes = (minutes // 5) * 5
            candle_timestamp = rounded_time.replace(minute=rounded_minutes)

            return {
                'timestamp': candle_timestamp,
                'symbol': symbol,
                'update_count': int(kline[8]),  # Number of trades as proxy for updates
                'bid_open': float(kline[1]),     # Open price
                'bid_high': float(kline[2]),     # High price
                'bid_low': float(kline[3]),      # Low price
                'bid_close': float(kline[4]),    # Close price
                'ask_open': float(kline[1]),     # Assume same as bid for historical data
                'ask_high': float(kline[2]),
                'ask_low': float(kline[3]),
                'ask_close': float(kline[4]),
                'mid_open': float(kline[1]),     # Mid price approximation
                'mid_high': float(kline[2]),
                'mid_low': float(kline[3]),
                'mid_close': float(kline[4]),
                'mid_mean': (float(kline[1]) + float(kline[4])) / 2,  # Average of open/close
                'mid_std': abs(float(kline[2]) - float(kline[3])) / 4,  # Rough volatility estimate
                'spread_mean': 0.0001,  # Conservative estimate for historical data
                'spread_max': 0.001,
                'spread_min': 0.00005,
                'spread_std': 0.0002,
                'spread_pct_mean': 0.0001,  # 0.01% average spread estimate
                'spread_pct_max': 0.001,
                'spread_pct_min': 0.00005,
                'spread_pct_std': 0.0002,
                'bid_volume_total': float(kline[5]) * 0.4,  # Estimate bid volume
                'ask_volume_total': float(kline[5]) * 0.6,  # Estimate ask volume
                'total_volume_total': float(kline[5]),
                'candle_range': float(kline[2]) - float(kline[3]),  # High - Low
                'candle_range_pct': ((float(kline[2]) - float(kline[3])) / float(kline[1])) * 100,
                'volume_imbalance': (float(kline[5]) * 0.4) - (float(kline[5]) * 0.6),  # Bid - Ask volume
                'volume_imbalance_pct': ((float(kline[5]) * 0.4) - (float(kline[5]) * 0.6)) / float(kline[5]) * 100
            }

        except Exception as e:
            logger.error(f"Error converting kline to candle: {e}")
            return None

    def find_data_gaps(self, symbol: str) -> List[Tuple[datetime, datetime]]:
        """Find gaps in existing 5-minute candle data."""
        candle_file = self.orderbook_dir / symbol.lower() / f"{symbol.lower()}_orderbook_5min_candles.csv"

        if not candle_file.exists():
            logger.info(f"No existing data for {symbol}, treating as complete gap")
            # Return a reasonable historical window (last 30 days)
            end_time = datetime.now().replace(second=0, microsecond=0)
            start_time = end_time - timedelta(days=30)
            return [(start_time, end_time)]

        try:
            df = pd.read_csv(candle_file)
            if df.empty:
                logger.info(f"Empty file for {symbol}")
                end_time = datetime.now().replace(second=0, microsecond=0)
                start_time = end_time - timedelta(days=30)
                return [(start_time, end_time)]

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            # Find gaps larger than 10 minutes (allowing for some tolerance)
            gaps = []
            expected_interval = timedelta(minutes=5)

            for i in range(1, len(df)):
                current_time = df.iloc[i]['timestamp']
                previous_time = df.iloc[i-1]['timestamp']
                gap = current_time - previous_time

                if gap > expected_interval * 2:  # Gap larger than 10 minutes
                    gap_start = previous_time + expected_interval
                    gap_end = current_time - expected_interval
                    gaps.append((gap_start, gap_end))

            logger.info(f"Found {len(gaps)} gaps in {symbol} data")
            return gaps

        except Exception as e:
            logger.error(f"Error finding gaps for {symbol}: {e}")
            return []

    def recover_symbol_data(self, symbol: str, gaps: List[Tuple[datetime, datetime]]) -> int:
        """Recover data for a specific symbol by filling gaps."""
        if not gaps:
            logger.info(f"No gaps to fill for {symbol}")
            return 0

        candle_file = self.orderbook_dir / symbol.lower() / f"{symbol.lower()}_orderbook_5min_candles.csv"
        recovered_candles = []

        for gap_start, gap_end in gaps:
            logger.info(f"Recovering {symbol} data from {gap_start} to {gap_end}")

            # Fetch klines in chunks to avoid API limits
            current_start = gap_start
            while current_start < gap_end:
                current_end = min(current_start + timedelta(hours=12), gap_end)  # 12-hour chunks

                klines = self.fetch_historical_klines(
                    symbol=symbol,
                    interval='5m',
                    start_time=current_start,
                    end_time=current_end,
                    limit=500
                )

                if klines:
                    for kline in klines:
                        candle = self.kline_to_candle(kline, symbol)
                        if candle and gap_start <= candle['timestamp'] <= gap_end:
                            recovered_candles.append(candle)

                    logger.info(f"Recovered {len(klines)} klines for {symbol} ({current_start} to {current_end})")

                current_start = current_end

                # Small delay between chunks
                time.sleep(0.1)

        # Save recovered data
        if recovered_candles:
            recovery_df = pd.DataFrame(recovered_candles)
            recovery_df = recovery_df.sort_values('timestamp')

            # Remove duplicates with existing data
            if candle_file.exists():
                existing_df = pd.read_csv(candle_file)
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])

                # Combine and deduplicate
                combined_df = pd.concat([existing_df, recovery_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset='timestamp', keep='first')
                combined_df = combined_df.sort_values('timestamp')
                final_df = combined_df
            else:
                final_df = recovery_df

            # Save the combined data
            final_df.to_csv(candle_file, index=False)
            logger.info(f"Saved {len(recovery_df)} recovered candles for {symbol}")

            return len(recovery_df)

        return 0

    def recover_all_data(self) -> Dict[str, int]:
        """Recover missing data for all symbols."""
        logger.info("🔄 Starting data recovery for all symbols...")

        recovery_stats = {}

        for symbol in tqdm(self.quality_symbols, desc="Recovering data"):
            try:
                gaps = self.find_data_gaps(symbol)
                recovered_count = self.recover_symbol_data(symbol, gaps)
                recovery_stats[symbol] = recovered_count

                if recovered_count > 0:
                    logger.info(f"✅ Recovered {recovered_count} candles for {symbol}")
                else:
                    logger.info(f"ℹ️ No recovery needed for {symbol}")

            except Exception as e:
                logger.error(f"Error recovering data for {symbol}: {e}")
                recovery_stats[symbol] = 0

        return recovery_stats

    def generate_recovery_report(self, recovery_stats: Dict[str, int]):
        """Generate a comprehensive recovery report."""
        print("\n" + "="*80)
        print("📊 ORDERBOOK DATA RECOVERY REPORT")
        print("="*80)

        total_recovered = sum(recovery_stats.values())
        symbols_with_recovery = sum(1 for count in recovery_stats.values() if count > 0)

        print("\n📈 Recovery Statistics:")
        print(f"   Total Candles Recovered: {total_recovered:,}")
        print(f"   Symbols with Recovery: {symbols_with_recovery}/{len(self.quality_symbols)}")

        print("\n🔍 Per-Symbol Recovery:")
        for symbol, count in sorted(recovery_stats.items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                print(f"   {symbol}: {count:,} candles recovered")
            else:
                print(f"   {symbol}: No recovery needed")

        print("\n🎯 Recovery Method:")
        print("   • Used Binance Historical Klines API (fapi/v1/klines)")
        print("   • Fetched 5-minute OHLCV candle data")
        print("   • Converted to orderbook candle format with estimated metrics")
        print("   • Filled gaps larger than 10 minutes in existing data")

        print("\n💡 Data Quality Notes:")
        print("   • Historical klines provide accurate OHLCV price data")
        print("   • Volume data is real from executed trades")
        print("   • Spread and depth metrics are estimated approximations")
        print("   • Best suited for price action analysis during gaps")

        print("\n📁 Output Location:")
        print(f"   {self.orderbook_dir}/")
        print("   Each symbol directory contains recovered candle data")

        if total_recovered > 0:
            print("\n✅ SUCCESS: Data gaps have been filled!")
            print(f"   Recovered {total_recovered:,} historical candles across {symbols_with_recovery} symbols")
        else:
            print("\nℹ️ INFO: No significant data gaps found")
            print("   Your existing data appears to be complete")

        print("="*80)

def main():
    """Main recovery function."""
    recoverer = OrderbookDataRecovery()

    print("🔄 Starting Orderbook Data Recovery Process...")
    print("This will fill gaps in your 5-minute candle data using Binance historical APIs")
    print()

    recovery_stats = recoverer.recover_all_data()
    recoverer.generate_recovery_report(recovery_stats)

    # Save recovery statistics
    with open('data_recovery_stats.json', 'w') as f:
        json.dump({
            'recovery_timestamp': datetime.now().isoformat(),
            'recovery_stats': recovery_stats,
            'total_recovered': sum(recovery_stats.values())
        }, f, indent=2)

    print("💾 Recovery statistics saved to: data_recovery_stats.json")

if __name__ == "__main__":
    main()
