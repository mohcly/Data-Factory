"""
Database Manager for Data Fetching Bot
Handles database creation, initialization, and maintenance
"""

import os
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import shutil

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database creation, initialization, and maintenance"""

    def __init__(self, db_path: str = None):
        """Initialize database manager with fresh database creation"""
        self.db_path = db_path or self._generate_db_path()
        self.is_new_database = not os.path.exists(self.db_path)

        if self.is_new_database:
            logger.info(f"Creating brand new database: {self.db_path}")
            self._create_fresh_database()
        else:
            logger.info(f"Using existing database: {self.db_path}")
            self._validate_existing_database()

    def _generate_db_path(self) -> str:
        """Generate database path with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        data_dir = "/workspace/memory-bank/data-fetching-bot/data"
        os.makedirs(data_dir, exist_ok=True)
        return os.path.join(data_dir, f"crypto_data_{timestamp}.db")

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

        logger.info("✅ Tables created successfully")

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

        logger.info("✅ Indexes created successfully")

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

        logger.info("✅ Views created successfully")

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
        ]

        for key, value, type_ in config_defaults:
            conn.execute("""
                INSERT OR IGNORE INTO system_config
                (config_key, config_value, config_type)
                VALUES (?, ?, ?)
            """, (key, value, type_))

        logger.info("✅ Configuration initialized")

    def _validate_existing_database(self):
        """Validate existing database integrity"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check required tables
                required_tables = ['crypto_data', 'system_config', 'api_performance']
                for table in required_tables:
                    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
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

    def insert_crypto_data(self, data: List[Dict]) -> int:
        """Insert cryptocurrency data into database"""
        with sqlite3.connect(self.db_path) as conn:
            inserted_count = 0
            for record in data:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO crypto_data
                        (symbol, timestamp, open, high, low, close, volume, source_api, data_quality_score, is_validated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record['symbol'],
                        record['timestamp'],
                        record['open'],
                        record['high'],
                        record['low'],
                        record['close'],
                        record['volume'],
                        record.get('source_api', 'unknown'),
                        record.get('data_quality_score', 1.0),
                        record.get('is_validated', False)
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.error(f"Error inserting record {record}: {e}")
                    continue

            conn.commit()
            return inserted_count

    def update_api_performance(self, api_name: str, symbol: str, success: bool, response_time: float = None):
        """Update API performance metrics"""
        with sqlite3.connect(self.db_path) as conn:
            # Check if record exists
            cursor = conn.execute("""
                SELECT id, request_count, success_count, error_count, average_response_time
                FROM api_performance
                WHERE api_name = ? AND symbol = ?
            """, (api_name, symbol))

            existing = cursor.fetchone()

            if existing:
                # Update existing record
                api_id, req_count, success_count, error_count, avg_time = existing

                new_req_count = req_count + 1
                new_success_count = success_count + (1 if success else 0)
                new_error_count = error_count + (1 if not success else 0)
                new_avg_time = (avg_time * req_count + (response_time or 0)) / new_req_count if response_time else avg_time

                conn.execute("""
                    UPDATE api_performance
                    SET request_count = ?, success_count = ?, error_count = ?,
                        average_response_time = ?, last_success = ?, last_error = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (
                    new_req_count,
                    new_success_count,
                    new_error_count,
                    new_avg_time,
                    datetime.now() if success else None,
                    datetime.now() if not success else None,
                    api_id
                ))
            else:
                # Insert new record
                conn.execute("""
                    INSERT INTO api_performance
                    (api_name, symbol, request_count, success_count, error_count,
                     average_response_time, last_success, last_error)
                    VALUES (?, ?, 1, ?, ?, ?, ?, ?)
                """, (
                    api_name,
                    symbol,
                    1 if success else 0,
                    1 if not success else 0,
                    response_time or 0,
                    datetime.now() if success else None,
                    datetime.now() if not success else None
                ))

            conn.commit()

    def log_error(self, error_type: str, error_message: str, error_details: str = None,
                  component: str = None, severity: str = 'error'):
        """Log system errors"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO system_errors
                (error_type, error_message, error_details, component, severity)
                VALUES (?, ?, ?, ?, ?)
            """, (error_type, error_message, error_details, component, severity))
            conn.commit()

    def backup_database(self, backup_path: str = None) -> str:
        """Create a backup of the current database"""
        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.db_path}.backup_{timestamp}"

        try:
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"Database backup created: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Failed to create database backup: {e}")
            raise

    def get_config_value(self, key: str, default_value: str = None) -> str:
        """Get configuration value from database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config_value FROM system_config WHERE config_key = ?", (key,))
            result = cursor.fetchone()
            return result[0] if result else default_value

    def set_config_value(self, key: str, value: str, config_type: str = 'string'):
        """Set configuration value in database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO system_config
                (config_key, config_value, config_type, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (key, value, config_type))
            conn.commit()