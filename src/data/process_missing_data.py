#!/usr/bin/env python3
"""
Process Missing Data for Correlated Groups

This script processes the raw data files in Data-factory to get the missing
1+ year of data that's not in the processed files.

Usage:
    python process_missing_data.py

Author: Data Processing Script
Created: 2025-09-21
"""

import pandas as pd
import os
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
RAW_DATA_DIR = "/Users/mohamedcoulibaly/MVP/Crypto/Data-factory/data"
PROCESSED_DATA_DIR = "/Users/mohamedcoulibaly/MVP/Crypto/Data-factory/pre-processing/processed"
OUTPUT_DIR = "/Users/mohamedcoulibaly/MVP/Crypto/Data-factory/pre-processing/processed"

# Correlated group assets
CORRELATED_ASSETS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT']

class MissingDataProcessor:
    """
    Processes missing data from raw files to get the latest year of data.
    """

    def __init__(self):
        """Initialize missing data processor"""
        self.raw_data_dir = Path(RAW_DATA_DIR)
        self.processed_data_dir = Path(PROCESSED_DATA_DIR)
        self.output_dir = Path(OUTPUT_DIR)

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_latest_processed_date(self, symbol: str) -> str:
        """Get the latest date from processed data"""
        processed_file = self.processed_data_dir / f"{symbol}_1H_data.csv"

        if not processed_file.exists():
            return '2020-01-01'

        try:
            df = pd.read_csv(processed_file)
            return df['timestamp'].max()
        except Exception as e:
            logger.error(f"Error reading processed file for {symbol}: {e}")
            return '2020-01-01'

    def get_raw_data_file(self, symbol: str) -> Path:
        """Get the raw data file path for a symbol"""
        # Map symbols to available raw data
        raw_symbol_map = {
            'BTCUSDT': 'btcusdt',
            'ETHUSDT': 'ethusdt',
            'SOLUSDT': 'solusdt',  # May not exist
            'ADAUSDT': 'adausdt'   # May not exist
        }

        if symbol not in raw_symbol_map:
            return None

        raw_dir = self.raw_data_dir / raw_symbol_map[symbol]
        if not raw_dir.exists():
            return None

        # Look for 1h data file
        for file in raw_dir.glob(f"*{symbol.lower()}*1h*.csv"):
            return file

        # Look for any CSV file in the directory
        csv_files = list(raw_dir.glob("*.csv"))
        if csv_files:
            return csv_files[0]

        return None

    def process_raw_data(self, symbol: str) -> pd.DataFrame:
        """Process raw data file for a symbol"""
        raw_file = self.get_raw_data_file(symbol)

        if not raw_file:
            logger.warning(f"No raw data file found for {symbol}")
            return pd.DataFrame()

        try:
            logger.info(f"Processing raw data from: {raw_file}")

            # Read raw data
            df = pd.read_csv(raw_file)

            # Handle different column formats
            if 'timestamp' in df.columns:
                # Already has timestamp column
                pass
            elif 'open_time' in df.columns:
                # Convert open_time to timestamp
                df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
                df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                logger.error(f"No timestamp column found in {raw_file}")
                return pd.DataFrame()

            # Standardize column names
            column_mapping = {
                'timestamp': 'timestamp',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            }

            # Rename columns
            df = df.rename(columns=column_mapping)

            # Keep only required columns
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df = df[[col for col in required_columns if col in df.columns]]

            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Sort by timestamp
            df = df.sort_values('timestamp')

            # Remove duplicates
            df = df.drop_duplicates(subset=['timestamp'])

            logger.info(f"Processed {len(df)} rows for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Error processing raw data for {symbol}: {e}")
            return pd.DataFrame()

    def get_missing_data(self, symbol: str) -> pd.DataFrame:
        """Get data that's missing from processed files"""
        latest_processed = self.get_latest_processed_date(symbol)
        logger.info(f"Latest processed date for {symbol}: {latest_processed}")

        # Process raw data
        df = self.process_raw_data(symbol)

        if df.empty:
            return pd.DataFrame()

        # Filter for data after the latest processed date
        if latest_processed != '2020-01-01':
            cutoff_date = pd.to_datetime(latest_processed)
            missing_data = df[df['timestamp'] > cutoff_date]
        else:
            # No processed data, take all
            missing_data = df

        logger.info(f"Found {len(missing_data)} missing rows for {symbol}")
        return missing_data

    def update_processed_file(self, symbol: str) -> bool:
        """Update the processed data file with missing data"""
        missing_data = self.get_missing_data(symbol)

        if missing_data.empty:
            logger.info(f"No missing data found for {symbol}")
            return False

        processed_file = self.processed_data_dir / f"{symbol}_1H_data.csv"

        try:
            # Load existing processed data
            if processed_file.exists():
                existing_data = pd.read_csv(processed_file)
                existing_data['timestamp'] = pd.to_datetime(existing_data['timestamp'])

                # Combine data
                combined_data = pd.concat([existing_data, missing_data], ignore_index=True)
                combined_data = combined_data.drop_duplicates(subset=['timestamp'])
            else:
                combined_data = missing_data

            # Sort by timestamp
            combined_data = combined_data.sort_values('timestamp')

            # Save updated file
            output_file = self.output_dir / f"{symbol}_1H_data.csv"
            combined_data.to_csv(output_file, index=False)

            logger.info(f"Updated {symbol} processed file with {len(missing_data)} new rows")
            logger.info(f"Total rows in updated file: {len(combined_data)}")

            return True

        except Exception as e:
            logger.error(f"Error updating processed file for {symbol}: {e}")
            return False

    def process_all_assets(self) -> dict:
        """Process missing data for all correlated assets"""
        results = {}

        for symbol in CORRELATED_ASSETS:
            logger.info(f"Processing missing data for {symbol}")
            try:
                success = self.update_processed_file(symbol)
                results[symbol] = success
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                results[symbol] = False

        return results

    def get_data_statistics(self) -> dict:
        """Get statistics about the data"""
        stats = {}

        for symbol in CORRELATED_ASSETS:
            processed_file = self.processed_data_dir / f"{symbol}_1H_data.csv"

            if processed_file.exists():
                try:
                    df = pd.read_csv(processed_file)
                    stats[symbol] = {
                        'rows': len(df),
                        'min_date': df['timestamp'].min(),
                        'max_date': df['timestamp'].max(),
                        'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}"
                    }
                except Exception as e:
                    logger.error(f"Error reading stats for {symbol}: {e}")
                    stats[symbol] = {'rows': 0, 'min_date': None, 'max_date': None}
            else:
                stats[symbol] = {'rows': 0, 'min_date': None, 'max_date': None}

        return stats

def main():
    """Main execution function"""
    logger.info("Starting missing data processing for correlated groups")
    logger.info(f"Raw data directory: {RAW_DATA_DIR}")
    logger.info(f"Processed data directory: {PROCESSED_DATA_DIR}")

    processor = MissingDataProcessor()

    try:
        # Process all assets
        logger.info("Processing missing data for all assets...")
        results = processor.process_all_assets()

        # Get final statistics
        stats = processor.get_data_statistics()

        # Print summary
        print("\n" + "="*70)
        print("MISSING DATA PROCESSING SUMMARY")
        print("="*70)

        for symbol in CORRELATED_ASSETS:
            print(f"{symbol}:")
            print(f"  Processing successful: {results[symbol]}")
            print(f"  Total rows: {stats[symbol]['rows']}")
            print(f"  Date range: {stats[symbol]['date_range']}")
            print()

        print("="*70)
        print("Processing completed!")
        print("="*70)

    except Exception as e:
        logger.error(f"Error during missing data processing: {e}")
        raise

if __name__ == "__main__":
    main()
