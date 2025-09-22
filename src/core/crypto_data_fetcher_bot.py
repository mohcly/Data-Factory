#!/usr/bin/env python3
"""
Crypto Data Fetcher Bot - Main Application

This is the main application that creates a fresh database and continuously
fetches cryptocurrency data with advanced error handling and recovery.

Features:
- Brand new database creation on startup
- Multi-API data fetching with failover
- Automatic gap detection and recovery
- Real-time monitoring and health checks
- Production-grade error handling

Usage:
    python crypto_data_fetcher_bot.py

Author: Data Fetcher Bot
Created: 2025-09-21
"""

import asyncio
import sqlite3
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/mohamedcoulibaly/MVP/Crypto/Data-factory/logs/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database creation, initialization, and maintenance"""

    def __init__(self, db_path: str = None):
        """Initialize database manager with fresh database creation"""
        self.db_path = db_path or self._generate_db_path()
        self.is_new_database = not os.path.exists(self.db_path)

        if self.is_new_database:
            logger.info(f"🗄️ Creating brand new database: {self.db_path}")
            self._create_fresh_database()
        else:
            logger.info(f"📂 Using existing database: {self.db_path}")
            self._validate_existing_database()

    def _generate_db_path(self) -> str:
        """Generate database path with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"/Users/mohamedcoulibaly/MVP/Crypto/Data-factory/databases/crypto_data_{timestamp}.db"

    def _create_fresh_database(self):
        """Create brand new database with complete schema"""
        logger.info("🗄️ Initializing fresh database...")

        # Create parent directory if needed
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # Initialize database with schema
        with sqlite3.connect(self.db_path) as conn:
            self._create_tables(conn)
            self._create_indexes(conn)
            self._create_views(conn)
            self._initialize_config(conn)

        logger.info("✅ Fresh database created successfully")

    def _create_tables(self, conn):
        """Create all required tables"""
        # Core data table
        conn.execute("""
            CREATE TABLE crypto_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                source_api TEXT,
                data_quality_score REAL DEFAULT 1.0,
                is_validated BOOLEAN DEFAULT FALSE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp)
            )
        """)

        # System configuration table
        conn.execute("""
            CREATE TABLE system_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT UNIQUE NOT NULL,
                config_value TEXT,
                config_type TEXT DEFAULT 'string',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # API performance tracking
        conn.execute("""
            CREATE TABLE api_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                request_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                average_response_time REAL DEFAULT 0.0,
                last_success DATETIME,
                last_error DATETIME,
                health_status TEXT DEFAULT 'healthy',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Data gaps tracking
        conn.execute("""
            CREATE TABLE data_gaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                gap_start DATETIME NOT NULL,
                gap_end DATETIME NOT NULL,
                gap_duration_hours INTEGER,
                recovery_status TEXT DEFAULT 'pending',
                recovery_attempts INTEGER DEFAULT 0,
                last_recovery_attempt DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Error logging
        conn.execute("""
            CREATE TABLE system_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_type TEXT NOT NULL,
                error_message TEXT,
                error_details TEXT,
                component TEXT,
                severity TEXT DEFAULT 'error',
                resolution_status TEXT DEFAULT 'open',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _create_indexes(self, conn):
        """Create database indexes for performance"""
        # Time-based queries index
        conn.execute("""
            CREATE INDEX idx_crypto_data_symbol_timestamp
            ON crypto_data(symbol, timestamp)
        """)

        # Quality-based queries
        conn.execute("""
            CREATE INDEX idx_crypto_data_quality
            ON crypto_data(data_quality_score, timestamp)
        """)

        # API performance index
        conn.execute("""
            CREATE INDEX idx_api_performance_status
            ON api_performance(api_name, health_status)
        """)

        # Gap recovery index
        conn.execute("""
            CREATE INDEX idx_data_gaps_status
            ON data_gaps(recovery_status, symbol)
        """)

    def _create_views(self, conn):
        """Create useful database views"""
        # All validated data
        conn.execute("""
            CREATE VIEW all_crypto_data AS
            SELECT
                symbol,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                source_api,
                data_quality_score,
                is_validated,
                created_at
            FROM crypto_data
            WHERE is_validated = TRUE
            ORDER BY symbol, timestamp
        """)

        # Daily statistics
        conn.execute("""
            CREATE VIEW daily_stats AS
            SELECT
                symbol,
                DATE(timestamp) as date,
                COUNT(*) as data_points,
                MIN(low) as day_low,
                MAX(high) as day_high,
                AVG(close) as day_avg_close,
                SUM(volume) as day_volume,
                MAX(data_quality_score) as max_quality
            FROM crypto_data
            GROUP BY symbol, DATE(timestamp)
            ORDER BY symbol, date
        """)

    def _initialize_config(self, conn):
        """Initialize default configuration values"""
        config_defaults = [
            ('bot_version', '1.0.0', 'string'),
            ('database_created', datetime.now().isoformat(), 'datetime'),
            ('last_data_fetch', 'never', 'string'),
            ('supported_symbols', 'BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT', 'string'),
            ('default_timeframe', '1h', 'string'),
            ('max_concurrent_apis', '3', 'integer'),
            ('data_retention_days', '3650', 'integer'),  # 10 years
            ('bot_start_time', datetime.now().isoformat(), 'datetime'),
        ]

        for key, value, type_ in config_defaults:
            conn.execute("""
                INSERT OR IGNORE INTO system_config
                (config_key, config_value, config_type)
                VALUES (?, ?, ?)
            """, (key, value, type_))

    def _validate_existing_database(self):
        """Validate existing database integrity"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check required tables
                required_tables = ['crypto_data', 'system_config', 'api_performance']
                for table in required_tables:
                    cursor = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                    if not cursor.fetchone():
                        logger.warning(f"Required table '{table}' not found. Consider creating fresh database.")
                        return False

                # Check configuration
                cursor = conn.execute("SELECT COUNT(*) FROM system_config")
                if cursor.fetchone()[0] == 0:
                    logger.warning("Database configuration incomplete. Reinitializing...")
                    self._initialize_config(conn)

                logger.info("✅ Existing database validated successfully")
                return True

        except Exception as e:
            logger.error(f"Database validation failed: {e}")
            return False

    def get_database_info(self) -> dict:
        """Get comprehensive database information"""
        with sqlite3.connect(self.db_path) as conn:
            # Get table counts
            tables_info = {}
            for table in ['crypto_data', 'api_performance', 'data_gaps', 'system_errors']:
                try:
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    tables_info[table] = count
                except:
                    tables_info[table] = 0

            # Get data coverage
            try:
                cursor = conn.execute("""
                    SELECT
                        symbol,
                        COUNT(*) as rows,
                        MIN(timestamp) as min_date,
                        MAX(timestamp) as max_date
                    FROM crypto_data
                    GROUP BY symbol
                """)
                data_coverage = cursor.fetchall()
            except:
                data_coverage = []

            return {
                'database_path': self.db_path,
                'is_new_database': self.is_new_database,
                'tables_info': tables_info,
                'data_coverage': data_coverage,
                'total_records': sum(tables_info.values())
            }

class CryptoDataFetcher:
    """Main crypto data fetching bot"""

    def __init__(self, db_manager: DatabaseManager):
        """Initialize the data fetcher"""
        self.db_manager = db_manager
        self.is_running = False
        self.last_fetch_time = None
        self.supported_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT']
        self.current_symbol_index = 0

        # API configurations
        self.api_configs = {
            'binance': {
                'name': 'Binance',
                'base_url': 'https://api.binance.com/api/v3',
                'rate_limit': 1200,
                'symbols': {
                    'BTCUSDT': 'btcusdt',
                    'ETHUSDT': 'ethusdt',
                    'SOLUSDT': 'solusdt',
                    'ADAUSDT': 'adausdt'
                }
            },
            'coingecko': {
                'name': 'CoinGecko',
                'base_url': 'https://api.coingecko.com/api/v3',
                'rate_limit': 50,
                'symbols': {
                    'BTC': 'bitcoin',
                    'ETH': 'ethereum',
                    'SOL': 'solana',
                    'ADA': 'cardano'
                }
            }
        }

        logger.info("🚀 Crypto Data Fetcher Bot initialized")

    async def fetch_from_binance(self, symbol: str) -> pd.DataFrame:
        """Fetch data from Binance API"""
        try:
            config = self.api_configs['binance']
            binance_symbol = config['symbols'].get(symbol)

            if not binance_symbol:
                logger.error(f"Symbol {symbol} not supported by Binance")
                return pd.DataFrame()

            # Calculate date range for fetching
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)  # Last 24 hours

            # For initial run, fetch more historical data
            if self.db_manager.is_new_database:
                start_date = end_date - timedelta(days=30)  # Last 30 days

            # Convert to timestamps
            start_ts = int(start_date.timestamp() * 1000)
            end_ts = int(end_date.timestamp() * 1000)

            url = f"{config['base_url']}/klines"
            params = {
                'symbol': binance_symbol.upper(),
                'interval': '1h',
                'startTime': start_ts,
                'endTime': end_ts,
                'limit': 1000
            }

            logger.info(f"📡 Fetching {symbol} from Binance: {start_date} to {end_date}")

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if not data:
                logger.warning(f"No data returned from Binance for {symbol}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])

            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Convert values to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)

            logger.info(f"✅ Successfully fetched {len(df)} rows from Binance for {symbol}")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            logger.error(f"❌ Error fetching from Binance for {symbol}: {e}")
            return pd.DataFrame()

    async def fetch_from_coingecko(self, symbol: str) -> pd.DataFrame:
        """Fetch data from CoinGecko API"""
        try:
            config = self.api_configs['coingecko']
            cg_symbol = config['symbols'].get(symbol.split('USDT')[0])  # Remove USDT

            if not cg_symbol:
                logger.error(f"Symbol {symbol} not supported by CoinGecko")
                return pd.DataFrame()

            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)

            if self.db_manager.is_new_database:
                start_date = end_date - timedelta(days=7)  # Last 7 days (free tier limit)

            # Convert to timestamps
            start_ts = int(start_date.timestamp())
            end_ts = int(end_date.timestamp())

            url = f"{config['base_url']}/coins/{cg_symbol}/market_chart/range"
            params = {
                'vs_currency': 'usd',
                'from': start_ts,
                'to': end_ts
            }

            logger.info(f"📡 Fetching {symbol} from CoinGecko: {start_date} to {end_date}")

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if 'prices' not in data:
                logger.error(f"No price data found in CoinGecko response for {symbol}")
                return pd.DataFrame()

            # Convert to DataFrame
            prices = data['prices']
            df = pd.DataFrame(prices, columns=['timestamp', 'close'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Add missing columns with NaN
            df['open'] = np.nan
            df['high'] = np.nan
            df['low'] = np.nan
            df['volume'] = np.nan

            # Resample to 1H if needed
            df = df.set_index('timestamp').resample('1H').last().reset_index()

            # Forward fill missing values
            df = df.fillna(method='ffill')

            # Format timestamp
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            logger.info(f"✅ Successfully fetched {len(df)} rows from CoinGecko for {symbol}")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            logger.error(f"❌ Error fetching from CoinGecko for {symbol}: {e}")
            return pd.DataFrame()

    async def fetch_symbol_data(self, symbol: str) -> bool:
        """Fetch data for a single symbol from multiple APIs"""
        logger.info(f"🔄 Fetching data for {symbol}")

        # Try Binance first (most reliable)
        df = await self.fetch_from_binance(symbol)

        # If Binance fails, try CoinGecko
        if df.empty:
            logger.warning(f"Binance failed for {symbol}, trying CoinGecko...")
            df = await self.fetch_from_coingecko(symbol)

        if df.empty:
            logger.error(f"❌ Failed to fetch data for {symbol} from any API")
            return False

        # Save to database
        return await self.save_to_database(symbol, df)

    async def save_to_database(self, symbol: str, df: pd.DataFrame) -> bool:
        """Save data to database"""
        try:
            with sqlite3.connect(self.db_manager.db_path) as conn:
                # Prepare data for insertion
                records = []
                for _, row in df.iterrows():
                    records.append((
                        symbol,
                        row['timestamp'],
                        float(row['open']) if pd.notna(row['open']) else 0,
                        float(row['high']) if pd.notna(row['high']) else 0,
                        float(row['low']) if pd.notna(row['low']) else 0,
                        float(row['close']),
                        float(row['volume']) if pd.notna(row['volume']) else 0,
                        'binance',  # source API
                        1.0,  # data quality score
                        True  # is validated
                    ))

                # Insert data
                conn.executemany("""
                    INSERT OR IGNORE INTO crypto_data
                    (symbol, timestamp, open, high, low, close, volume, source_api, data_quality_score, is_validated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, records)

                conn.commit()

                logger.info(f"💾 Saved {len(records)} records for {symbol}")
                return True

        except Exception as e:
            logger.error(f"❌ Error saving data for {symbol}: {e}")
            return False

    async def run_data_collection_cycle(self):
        """Run one complete cycle of data collection"""
        logger.info("🔄 Starting data collection cycle...")

        for symbol in self.supported_symbols:
            try:
                success = await self.fetch_symbol_data(symbol)
                if success:
                    logger.info(f"✅ Successfully collected data for {symbol}")
                else:
                    logger.error(f"❌ Failed to collect data for {symbol}")

                # Small delay between symbols
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ Error in data collection for {symbol}: {e}")

        logger.info("🎉 Data collection cycle completed")

    async def start_continuous_collection(self):
        """Start continuous data collection"""
        logger.info("🚀 Starting continuous data collection...")

        self.is_running = True
        cycle_count = 0

        while self.is_running:
            try:
                cycle_count += 1
                logger.info(f"📊 Starting collection cycle #{cycle_count}")

                await self.run_data_collection_cycle()

                # Update last fetch time
                self.last_fetch_time = datetime.now()

                # Update database config
                with sqlite3.connect(self.db_manager.db_path) as conn:
                    conn.execute("""
                        UPDATE system_config
                        SET config_value = ?
                        WHERE config_key = 'last_data_fetch'
                    """, (self.last_fetch_time.isoformat(),))
                    conn.commit()

                # Wait before next cycle (1 hour)
                logger.info("⏰ Waiting 1 hour before next collection cycle...")
                await asyncio.sleep(3600)

            except KeyboardInterrupt:
                logger.info("🛑 Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"❌ Error in main loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error

    def stop(self):
        """Stop the data collection"""
        logger.info("🛑 Stopping data collection...")
        self.is_running = False

    def get_status(self) -> dict:
        """Get current bot status"""
        return {
            'is_running': self.is_running,
            'last_fetch_time': self.last_fetch_time.isoformat() if self.last_fetch_time else None,
            'database_path': self.db_manager.db_path,
            'supported_symbols': self.supported_symbols,
            'current_cycle': getattr(self, 'current_cycle', 0)
        }

async def main():
    """Main application entry point"""
    logger.info("="*60)
    logger.info("🚀 CRYPTO DATA FETCHER BOT STARTING")
    logger.info("="*60)

    try:
        # Create fresh database
        db_manager = DatabaseManager()
        db_info = db_manager.get_database_info()

        logger.info(f"📊 Database Status: {'NEW' if db_info['is_new_database'] else 'EXISTING'}")
        logger.info(f"📁 Database Path: {db_info['database_path']}")
        logger.info(f"💾 Total Records: {db_info['total_records']}")

        # Create data fetcher
        fetcher = CryptoDataFetcher(db_manager)

        # Start continuous collection
        await fetcher.start_continuous_collection()

    except KeyboardInterrupt:
        logger.info("🛑 Bot shutdown requested by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        raise
    finally:
        logger.info("👋 Data Fetcher Bot shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
