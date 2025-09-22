#!/usr/bin/env python3
"""
Create Database Tables for Correlated Groups Data

This script creates the database tables for the correlated groups data
in the Timeseries-forcasting database.

Usage:
    python create_correlated_groups_tables.py

Author: Database Setup Script
Created: 2025-09-21
"""

import sqlite3
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TARGET_DB_PATH = "/Users/mohamedcoulibaly/MVP/Crypto/Timeseries-forcasting/databases/correlated_groups_data.db"

# Correlated group assets
CORRELATED_ASSETS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT']

def create_database_tables():
    """Create all database tables for correlated groups"""

    # Ensure target directory exists
    os.makedirs(os.path.dirname(TARGET_DB_PATH), exist_ok=True)

    # Connect to database
    conn = sqlite3.connect(TARGET_DB_PATH)
    cursor = conn.cursor()

    logger.info(f"Creating database tables in: {TARGET_DB_PATH}")

    # Create table for each asset
    for symbol in CORRELATED_ASSETS:
        table_name = symbol.lower()

        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create table with proper schema
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp)
            )
        """)

        # Create index on timestamp for better performance
        cursor.execute(f"""
            CREATE INDEX idx_{table_name}_timestamp
            ON {table_name}(timestamp)
        """)

        logger.info(f"Created table: {table_name}")

    # Create a view for all correlated groups data
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS correlated_groups_all_data AS
        SELECT 'BTC' as symbol, timestamp, open, high, low, close, volume FROM btc
        UNION ALL
        SELECT 'ETH' as symbol, timestamp, open, high, low, close, volume FROM eth
        UNION ALL
        SELECT 'SOL' as symbol, timestamp, open, high, low, close, volume FROM sol
        UNION ALL
        SELECT 'ADA' as symbol, timestamp, open, high, low, close, volume FROM ada
        ORDER BY timestamp
    """)

    logger.info("Created correlated_groups_all_data view")

    # Create HMM models metadata table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hmm_models_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            model_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            parameters TEXT,
            training_data_start DATE,
            training_data_end DATE,
            performance_metrics TEXT,
            UNIQUE(model_name, symbol)
        )
    """)

    logger.info("Created hmm_models_metadata table")

    # Commit changes
    conn.commit()

    # Verify tables were created
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    logger.info(f"Created {len(tables)} tables:")
    for table in tables:
        logger.info(f"  - {table[0]}")

    # Close connection
    conn.close()

    logger.info("Database table creation completed successfully!")

def main():
    """Main execution function"""
    logger.info("Starting database table creation for correlated groups")
    logger.info(f"Target database: {TARGET_DB_PATH}")

    try:
        create_database_tables()

        print("\n" + "="*60)
        print("DATABASE TABLES CREATED SUCCESSFULLY")
        print("="*60)
        print(f"Database: {TARGET_DB_PATH}")
        print()
        print("Tables created:")
        for symbol in CORRELATED_ASSETS:
            print(f"  - {symbol.lower()}")
        print("  - hmm_models_metadata")
        print("  - correlated_groups_all_data (view)")
        print()
        print("Each table includes:")
        print("  - id (PRIMARY KEY)")
        print("  - timestamp (DATETIME, UNIQUE)")
        print("  - open, high, low, close (REAL)")
        print("  - volume (REAL)")
        print("  - created_at (DEFAULT CURRENT_TIMESTAMP)")
        print("  - Index on timestamp for performance")
        print("="*60)

    except Exception as e:
        logger.error(f"Error during table creation: {e}")
        raise

if __name__ == "__main__":
    main()
