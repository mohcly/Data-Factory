#!/usr/bin/env python3
"""
Convert High-Frequency Orderbook Data to 5-Minute Candles

This script aggregates real-time orderbook snapshots into 5-minute candles,
similar to traditional OHLCV candle data but with orderbook-specific metrics.

Features:
- Converts all cryptocurrencies in the dataset
- Creates OHLC (Open/High/Low/Close) for bid/ask prices
- Aggregates volume data over 5-minute intervals
- Calculates spread and mid-price statistics
- Maintains data quality and integrity

Author: MVP Crypto Data Factory
Created: 2025-10-19
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging
from tqdm import tqdm
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/orderbook_to_5min_conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OrderbookToCandlesConverter:
    """Convert high-frequency orderbook data to 5-minute candles."""

    def __init__(self, input_dir: str = "data/realtime_orderbooks",
                 output_dir: str = "data/orderbook_5min_candles"):
        """
        Initialize the converter.

        Args:
            input_dir: Directory containing orderbook CSV files
            output_dir: Directory to save 5-minute candle data
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Track conversion statistics
        self.stats = {
            'total_files_processed': 0,
            'total_records_input': 0,
            'total_candles_output': 0,
            'symbols_processed': []
        }

    def find_orderbook_files(self):
        """Find all orderbook CSV files to process."""
        csv_files = []
        if self.input_dir.exists():
            for csv_file in self.input_dir.rglob("*_orderbook_realtime.csv"):
                if csv_file.stat().st_size > 0:  # Only process non-empty files
                    csv_files.append(csv_file)
        return sorted(csv_files)

    def load_orderbook_data(self, csv_file: Path) -> pd.DataFrame:
        """Load and preprocess orderbook data from CSV file."""
        try:
            df = pd.read_csv(csv_file)

            if df.empty:
                logger.warning(f"Empty file: {csv_file}")
                return pd.DataFrame()

            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Set timestamp as index for resampling
            df = df.set_index('timestamp')

            # Ensure numeric columns are properly typed
            numeric_cols = ['best_bid', 'best_ask', 'spread', 'mid_price', 'spread_pct',
                          'bid_volume_top10', 'ask_volume_top10', 'total_volume_top10']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"Loaded {len(df)} records from {csv_file.name}")
            return df

        except Exception as e:
            logger.error(f"Error loading {csv_file}: {e}")
            return pd.DataFrame()

    def create_5min_candles(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Create 5-minute candles from orderbook data."""

        if df.empty:
            return pd.DataFrame()

        try:
            # Resample to 5-minute intervals
            candles = df.resample('5min').agg({
                # OHLC for best_bid
                'best_bid': ['first', 'max', 'min', 'last'],

                # OHLC for best_ask
                'best_ask': ['first', 'max', 'min', 'last'],

                # Spread statistics
                'spread': ['mean', 'max', 'min', 'std'],

                # Mid price statistics
                'mid_price': ['first', 'max', 'min', 'last', 'mean', 'std'],

                # Spread percentage statistics
                'spread_pct': ['mean', 'max', 'min', 'std'],

                # Volume aggregation
                'bid_volume_top10': 'sum',
                'ask_volume_top10': 'sum',
                'total_volume_top10': 'sum',

                # Count of updates in this candle
                'symbol': 'count'  # This will give us the number of records
            }).dropna()

            # Flatten column names
            candles.columns = [
                'bid_open', 'bid_high', 'bid_low', 'bid_close',
                'ask_open', 'ask_high', 'ask_low', 'ask_close',
                'spread_mean', 'spread_max', 'spread_min', 'spread_std',
                'mid_open', 'mid_high', 'mid_low', 'mid_close', 'mid_mean', 'mid_std',
                'spread_pct_mean', 'spread_pct_max', 'spread_pct_min', 'spread_pct_std',
                'bid_volume_total', 'ask_volume_total', 'total_volume_total',
                'update_count'
            ]

            # Reset index to get timestamp as column
            candles = candles.reset_index()

            # Add symbol column
            candles['symbol'] = symbol.upper()

            # Reorder columns for better readability
            column_order = [
                'timestamp', 'symbol', 'update_count',
                'bid_open', 'bid_high', 'bid_low', 'bid_close',
                'ask_open', 'ask_high', 'ask_low', 'ask_close',
                'mid_open', 'mid_high', 'mid_low', 'mid_close', 'mid_mean', 'mid_std',
                'spread_mean', 'spread_max', 'spread_min', 'spread_std',
                'spread_pct_mean', 'spread_pct_max', 'spread_pct_min', 'spread_pct_std',
                'bid_volume_total', 'ask_volume_total', 'total_volume_total'
            ]

            candles = candles[column_order]

            # Calculate additional useful metrics
            candles['candle_range'] = candles['ask_high'] - candles['bid_low']  # Total price range
            candles['candle_range_pct'] = (candles['candle_range'] / candles['mid_open']) * 100
            candles['volume_imbalance'] = candles['bid_volume_total'] - candles['ask_volume_total']
            candles['volume_imbalance_pct'] = (candles['volume_imbalance'] / (candles['bid_volume_total'] + candles['ask_volume_total'])) * 100

            logger.info(f"Created {len(candles)} 5-minute candles for {symbol}")
            return candles

        except Exception as e:
            logger.error(f"Error creating 5-minute candles for {symbol}: {e}")
            return pd.DataFrame()

    def save_candles(self, candles: pd.DataFrame, symbol: str):
        """Save the 5-minute candles to CSV file."""
        if candles.empty:
            return

        try:
            # Create symbol directory
            symbol_dir = self.output_dir / symbol.lower()
            symbol_dir.mkdir(exist_ok=True)

            # Save to CSV
            output_file = symbol_dir / f"{symbol.lower()}_orderbook_5min_candles.csv"
            candles.to_csv(output_file, index=False)

            # Also create a compressed version
            import gzip
            compressed_file = symbol_dir / f"{symbol.lower()}_orderbook_5min_candles.csv.gz"
            with gzip.open(compressed_file, 'wt') as f:
                candles.to_csv(f, index=False)

            logger.info(f"Saved {len(candles)} candles to {output_file}")
            logger.info(f"Compressed version: {compressed_file}")

        except Exception as e:
            logger.error(f"Error saving candles for {symbol}: {e}")

    def process_all_files(self):
        """Process all orderbook files and convert to 5-minute candles."""
        logger.info("🔄 Starting orderbook to 5-minute candles conversion...")

        csv_files = self.find_orderbook_files()
        logger.info(f"Found {len(csv_files)} orderbook files to process")

        if not csv_files:
            logger.warning("No orderbook files found!")
            return

        for csv_file in tqdm(csv_files, desc="Processing files"):
            try:
                # Extract symbol from filename
                symbol = csv_file.stem.replace('_orderbook_realtime', '').upper()

                # Load data
                df = self.load_orderbook_data(csv_file)
                if df.empty:
                    continue

                # Create 5-minute candles
                candles = self.create_5min_candles(df, symbol)
                if candles.empty:
                    continue

                # Save results
                self.save_candles(candles, symbol)

                # Update statistics
                self.stats['total_files_processed'] += 1
                self.stats['total_records_input'] += len(df)
                self.stats['total_candles_output'] += len(candles)
                self.stats['symbols_processed'].append(symbol)

            except Exception as e:
                logger.error(f"Error processing {csv_file}: {e}")
                continue

        self.print_summary()

    def print_summary(self):
        """Print conversion summary."""
        print("\n" + "="*80)
        print("🏁 ORDERBOOK TO 5-MINUTE CANDLES CONVERSION COMPLETE")
        print("="*80)

        print("📊 Conversion Statistics:")
        print(f"   Files Processed: {self.stats['total_files_processed']}")
        print(f"   Symbols: {', '.join(self.stats['symbols_processed'])}")
        print(f"   Input Records: {self.stats['total_records_input']:,}")
        print(f"   Output Candles: {self.stats['total_candles_output']:,}")

        if self.stats['total_records_input'] > 0:
            reduction_pct = (1 - self.stats['total_candles_output'] / self.stats['total_records_input']) * 100
            print(f"   Data Reduction: {reduction_pct:.1f}%")

        print(f"\n📁 Output Location: {self.output_dir}")

        # Show file sizes
        total_size_mb = 0
        for symbol in self.stats['symbols_processed']:
            csv_file = self.output_dir / symbol.lower() / f"{symbol.lower()}_orderbook_5min_candles.csv"
            if csv_file.exists():
                size_mb = csv_file.stat().st_size / (1024 * 1024)
                total_size_mb += size_mb
                print(f"   {symbol}: {size_mb:.2f} MB")

        print(f"   Total Size: {total_size_mb:.2f} MB")
        print("\n🎯 Next Steps:")
        print("  • Analyze 5-minute orderbook patterns")
        print("  • Study price action with depth context")
        print("  • Use for algorithmic trading strategies")
        print("  • Monitor market microstructure changes")
        print("="*80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Convert orderbook data to 5-minute candles')
    parser.add_argument('--input', default='data/realtime_orderbooks',
                       help='Input directory with orderbook CSV files')
    parser.add_argument('--output', default='data/orderbook_5min_candles',
                       help='Output directory for 5-minute candles')
    parser.add_argument('--symbol', help='Process only specific symbol (e.g., BTC)')

    args = parser.parse_args()

    # Create converter
    converter = OrderbookToCandlesConverter(args.input, args.output)

    if args.symbol:
        # Process single symbol
        csv_file = Path(args.input) / args.symbol.lower() / f"{args.symbol.lower()}_orderbook_realtime.csv"
        if csv_file.exists():
            df = converter.load_orderbook_data(csv_file)
            candles = converter.create_5min_candles(df, args.symbol.upper())
            converter.save_candles(candles, args.symbol.upper())
        else:
            print(f"❌ File not found: {csv_file}")
    else:
        # Process all files
        converter.process_all_files()


if __name__ == "__main__":
    main()
