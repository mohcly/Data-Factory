#!/usr/bin/env python3
"""
Update Correlated Groups Database with Latest Data

This script updates the correlated_groups_data.db database in Timeseries-forcasting
with the latest processed data from the Data-factory repository.

Usage:
    python update_correlated_groups_db.py

Author: Data Update Script
Created: 2025-09-21
"""

import pandas as pd
import sqlite3
import os
import logging
from pathlib import Path
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TARGET_DB_PATH = "/Users/mohamedcoulibaly/MVP/Crypto/Timeseries-forcasting/databases/correlated_groups_data.db"
PROCESSED_DATA_DIR = "/Users/mohamedcoulibaly/MVP/Crypto/Data-factory/pre-processing/processed"

# Correlated group assets (4 main assets)
CORRELATED_ASSETS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT']

class DatabaseUpdater:
    """
    Updates the correlated groups database with latest processed data.
    """

    def __init__(self):
        """Initialize database updater"""
        self.target_db_path = TARGET_DB_PATH
        self.processed_data_dir = Path(PROCESSED_DATA_DIR)

        # Ensure target directory exists
        os.makedirs(os.path.dirname(self.target_db_path), exist_ok=True)

        # Connect to target database
        self.conn = sqlite3.connect(self.target_db_path)
        self.conn.row_factory = sqlite3.Row

        logger.info(f"Connected to target database: {self.target_db_path}")

    def get_latest_timestamp(self, symbol: str) -> str:
        """Get the latest timestamp for a symbol in the database"""
        table_name = symbol.lower()

        try:
            cursor = self.conn.execute(f"SELECT MAX(timestamp) FROM {table_name}")
            result = cursor.fetchone()
            return result[0] if result[0] else '2020-01-01 00:00:00'
        except sqlite3.Error as e:
            logger.warning(f"Error getting latest timestamp for {symbol}: {e}")
            return '2020-01-01 00:00:00'

    def get_processed_file_path(self, symbol: str) -> Path:
        """Get the processed file path for a symbol"""
        # Try both naming conventions
        file_path1 = self.processed_data_dir / f"{symbol}_1H_data.csv"
        file_path2 = self.processed_data_dir / f"{symbol.split('USDT')[0]}_1H_data.csv"

        if file_path1.exists():
            return file_path1
        elif file_path2.exists():
            return file_path2
        else:
            # Return the standard path for logging
            return self.processed_data_dir / f"{symbol}_1H_data.csv"

    def load_processed_data(self, symbol: str) -> pd.DataFrame:
        """Load processed data for a symbol"""
        file_path = self.get_processed_file_path(symbol)

        if not file_path.exists():
            logger.error(f"Processed file not found: {file_path}")
            return pd.DataFrame()

        try:
            # Load CSV data
            df = pd.read_csv(file_path)

            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Sort by timestamp
            df = df.sort_values('timestamp')

            logger.info(f"Loaded {len(df)} rows for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Error loading data for {symbol}: {e}")
            return pd.DataFrame()

    def update_symbol_data(self, symbol: str) -> int:
        """Update data for a single symbol"""
        table_name = symbol.lower()

        # Get latest timestamp in database
        latest_db_timestamp = self.get_latest_timestamp(symbol)
        logger.info(f"Latest timestamp in {symbol} table: {latest_db_timestamp}")

        # Load processed data
        df = self.load_processed_data(symbol)

        if df.empty:
            logger.warning(f"No data to update for {symbol}")
            return 0

        # Filter for data newer than what's in database
        if latest_db_timestamp != '2020-01-01 00:00:00':
            cutoff_date = pd.to_datetime(latest_db_timestamp)
            new_data = df[df['timestamp'] > cutoff_date]
        else:
            # First time loading - take all data
            new_data = df

        if new_data.empty:
            logger.info(f"No new data to add for {symbol}")
            return 0

        logger.info(f"Adding {len(new_data)} new rows for {symbol}")

        # Insert new data
        rows_inserted = 0
        for _, row in new_data.iterrows():
            try:
                cursor = self.conn.execute(f"""
                    INSERT OR IGNORE INTO {table_name}
                    (timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    float(row['volume'])
                ))
                rows_inserted += cursor.rowcount

            except sqlite3.Error as e:
                logger.error(f"Error inserting row for {symbol}: {e}")
                continue

        self.conn.commit()
        logger.info(f"Successfully inserted {rows_inserted} rows for {symbol}")
        return rows_inserted

    def update_all_assets(self) -> dict:
        """Update all correlated group assets"""
        results = {}

        for symbol in CORRELATED_ASSETS:
            logger.info(f"Updating data for {symbol}")
            try:
                rows_inserted = self.update_symbol_data(symbol)
                results[symbol] = rows_inserted
            except Exception as e:
                logger.error(f"Error updating {symbol}: {e}")
                results[symbol] = 0

        return results

    def get_database_stats(self) -> dict:
        """Get database statistics"""
        stats = {}

        for symbol in CORRELATED_ASSETS:
            table_name = symbol.lower()
            try:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]

                cursor = self.conn.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table_name}")
                min_max = cursor.fetchone()

                stats[symbol] = {
                    'rows': count,
                    'min_date': min_max[0],
                    'max_date': min_max[1]
                }
            except sqlite3.Error as e:
                logger.error(f"Error getting stats for {symbol}: {e}")
                stats[symbol] = {'rows': 0, 'min_date': None, 'max_date': None}

        return stats

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main execution function"""
    logger.info("Starting correlated groups database update")
    logger.info(f"Target database: {TARGET_DB_PATH}")
    logger.info(f"Processed data directory: {PROCESSED_DATA_DIR}")

    updater = DatabaseUpdater()

    try:
        # Update all assets
        logger.info("Updating all correlated group assets...")
        results = updater.update_all_assets()

        # Get final statistics
        stats = updater.get_database_stats()

        # Print summary
        print("\n" + "="*60)
        print("DATABASE UPDATE SUMMARY")
        print("="*60)

        for symbol in CORRELATED_ASSETS:
            print(f"{symbol}:")
            print(f"  Rows inserted: {results[symbol]}")
            print(f"  Total rows: {stats[symbol]['rows']}")
            print(f"  Date range: {stats[symbol]['min_date']} to {stats[symbol]['max_date']}")
            print()

        print("="*60)
        print("Update completed successfully!")
        print("="*60)

    except Exception as e:
        logger.error(f"Error during database update: {e}")
        sys.exit(1)

    finally:
        updater.close()

if __name__ == "__main__":
    main()
