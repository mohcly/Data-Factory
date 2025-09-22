#!/usr/bin/env python3
"""
Fetch Missing Crypto Data from Multiple APIs

This script fetches missing SOL and ADA data using multiple cryptocurrency APIs:
- Binance API (existing)
- CoinGecko API (free)
- CoinMarketCap API (comprehensive)
- Yahoo Finance (alternative)

Usage:
    python fetch_missing_crypto_data.py

Author: Multi-API Data Fetcher
Created: 2025-09-21
"""

import pandas as pd
import numpy as np
import requests
import time
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
import yfinance as yf
from typing import Dict, List, Optional, Tuple
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultiAPIDataFetcher:
    """
    Fetches cryptocurrency data from multiple APIs to get missing data.
    """

    def __init__(self):
        """Initialize data fetcher with multiple API sources"""
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.output_dir = self.base_dir / "data" / "missing_data"

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # API configurations
        self.apis = {
            'coingecko': {
                'name': 'CoinGecko',
                'url': 'https://api.coingecko.com/api/v3',
                'free_tier': True,
                'rate_limit': 10,  # requests per minute
                'symbols': {
                    'SOL': 'solana',
                    'ADA': 'cardano'
                }
            },
            'coinmarketcap': {
                'name': 'CoinMarketCap',
                'url': 'https://pro-api.coinmarketcap.com/v1',
                'free_tier': False,  # Requires API key
                'rate_limit': 10,
                'symbols': {
                    'SOL': 'solana',
                    'ADA': 'cardano'
                }
            },
            'binance': {
                'name': 'Binance',
                'url': 'https://api.binance.com/api/v3',
                'free_tier': True,
                'rate_limit': 1200,  # requests per minute
                'symbols': {
                    'SOL': 'SOLUSDT',
                    'ADA': 'ADAUSDT'
                }
            }
        }

        logger.info("Multi-API data fetcher initialized")

    def fetch_coingecko_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch data from CoinGecko API (free tier)"""
        try:
            # Map our symbols to CoinGecko IDs
            cg_id = self.apis['coingecko']['symbols'].get(symbol)
            if not cg_id:
                logger.error(f"CoinGecko symbol mapping not found for {symbol}")
                return pd.DataFrame()

            # CoinGecko API endpoint for historical data
            url = f"{self.apis['coingecko']['url']}/coins/{cg_id}/market_chart/range"

            # Convert dates to timestamps
            start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

            params = {
                'vs_currency': 'usd',
                'from': start_ts,
                'to': end_ts
            }

            logger.info(f"Fetching {symbol} data from CoinGecko: {start_date} to {end_date}")

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if 'prices' not in data:
                logger.error(f"No price data found in CoinGecko response for {symbol}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data['prices'], columns=['timestamp', 'close'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Add missing columns with NaN (will be filled later)
            df['open'] = np.nan
            df['high'] = np.nan
            df['low'] = np.nan
            df['volume'] = np.nan

            # Resample to 1H if needed
            df = df.set_index('timestamp').resample('1H').last().reset_index()

            # Forward fill missing values
            df = df.fillna(method='ffill')

            logger.info(f"Successfully fetched {len(df)} rows from CoinGecko for {symbol}")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            logger.error(f"Error fetching from CoinGecko for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_yahoo_finance_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch data from Yahoo Finance"""
        try:
            # Map to Yahoo Finance symbols
            yahoo_symbols = {
                'SOL': 'SOL-USD',
                'ADA': 'ADA-USD'
            }

            yahoo_symbol = yahoo_symbols.get(symbol)
            if not yahoo_symbol:
                logger.error(f"Yahoo Finance symbol mapping not found for {symbol}")
                return pd.DataFrame()

            logger.info(f"Fetching {symbol} data from Yahoo Finance: {start_date} to {end_date}")

            # Download data
            df = yf.download(
                yahoo_symbol,
                start=start_date,
                end=end_date,
                interval='1h',
                progress=False
            )

            if df.empty:
                logger.warning(f"No data returned from Yahoo Finance for {symbol}")
                return pd.DataFrame()

            # Reset index to get timestamp as column
            df = df.reset_index()

            # Rename columns to match our format
            df = df.rename(columns={
                'Date': 'timestamp',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })

            # Ensure timestamp is in the right format
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            logger.info(f"Successfully fetched {len(df)} rows from Yahoo Finance for {symbol}")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            logger.error(f"Error fetching from Yahoo Finance for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_binance_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch data from Binance API"""
        try:
            from binance.client import Client
            client = Client()

            binance_symbol = self.apis['binance']['symbols'].get(symbol)
            if not binance_symbol:
                logger.error(f"Binance symbol mapping not found for {symbol}")
                return pd.DataFrame()

            logger.info(f"Fetching {symbol} data from Binance: {start_date} to {end_date}")

            # Convert dates to timestamps
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            # Get klines data
            klines = client.get_historical_klines(
                symbol=binance_symbol,
                interval=Client.KLINE_INTERVAL_1HOUR,
                start_str=start_date,
                end_str=end_date
            )

            if not klines:
                logger.warning(f"No klines data returned from Binance for {symbol}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])

            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Convert string values to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)

            logger.info(f"Successfully fetched {len(df)} rows from Binance for {symbol}")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            logger.error(f"Error fetching from Binance for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_all_apis(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Try to fetch data from all available APIs"""
        api_methods = [
            ('Yahoo Finance', self.fetch_yahoo_finance_data),
            ('CoinGecko', self.fetch_coingecko_data),
            ('Binance', self.fetch_binance_data)
        ]

        for api_name, method in api_methods:
            logger.info(f"Trying {api_name} for {symbol}...")

            try:
                df = method(symbol, start_date, end_date)

                if not df.empty and len(df) > 0:
                    logger.info(f"✅ Successfully fetched {len(df)} rows from {api_name} for {symbol}")
                    return df
                else:
                    logger.warning(f"⚠️ {api_name} returned empty data for {symbol}")

            except Exception as e:
                logger.error(f"❌ Error with {api_name} for {symbol}: {e}")
                continue

        logger.error(f"Failed to fetch data for {symbol} from any API")
        return pd.DataFrame()

    def get_missing_date_range(self, symbol: str) -> Tuple[str, str]:
        """Get the date range that's missing for a symbol"""
        # Check current database
        try:
            import sqlite3
            db_path = "/Users/mohamedcoulibaly/MVP/Crypto/Timeseries-forcasting/databases/correlated_groups_data.db"
            conn = sqlite3.connect(db_path)

            table_name = symbol.lower() + 'usdt'
            cursor = conn.execute(f"SELECT MAX(timestamp) FROM {table_name}")
            result = cursor.fetchone()

            if result and result[0]:
                start_date = result[0].split(' ')[0]  # Get date part
                # Add 1 day to avoid overlap
                start_dt = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)
                start_date = start_dt.strftime("%Y-%m-%d")
            else:
                start_date = "2024-02-20"  # Default if no data

            end_date = datetime.now().strftime("%Y-%m-%d")
            conn.close()

            return start_date, end_date

        except Exception as e:
            logger.error(f"Error getting missing date range for {symbol}: {e}")
            return "2024-02-20", datetime.now().strftime("%Y-%m-%d")

    def save_to_processed_format(self, symbol: str, df: pd.DataFrame) -> bool:
        """Save data in the same format as processed files"""
        try:
            # Ensure timestamp is in the right format
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            # Remove duplicates
            df = df.drop_duplicates(subset=['timestamp'])

            # Save in processed format
            output_file = self.base_dir / "pre-processing" / "processed" / f"{symbol}_1H_data.csv"

            # Load existing data if exists
            existing_df = pd.DataFrame()
            if output_file.exists():
                existing_df = pd.read_csv(output_file)
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])

                # Combine with new data
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['timestamp'])
                combined_df = combined_df.sort_values('timestamp')
            else:
                combined_df = df

            # Save combined data
            combined_df.to_csv(output_file, index=False)

            logger.info(f"✅ Saved {len(df)} new rows to {output_file}")
            logger.info(f"📊 Total rows in processed file: {len(combined_df)}")

            return True

        except Exception as e:
            logger.error(f"Error saving processed data for {symbol}: {e}")
            return False

    def fetch_and_save_missing_data(self, symbol: str) -> bool:
        """Fetch and save missing data for a symbol"""
        logger.info(f"Fetching missing data for {symbol}")

        # Get missing date range
        start_date, end_date = self.get_missing_date_range(symbol)
        logger.info(f"Date range for {symbol}: {start_date} to {end_date}")

        # Fetch data from multiple APIs
        df = self.fetch_all_apis(symbol, start_date, end_date)

        if df.empty:
            logger.error(f"Could not fetch any data for {symbol}")
            return False

        # Save in processed format
        success = self.save_to_processed_format(symbol, df)

        if success:
            logger.info(f"✅ Successfully updated {symbol} with {len(df)} new rows")

        return success

    def fetch_all_missing_data(self) -> Dict[str, bool]:
        """Fetch missing data for all symbols"""
        missing_symbols = ['SOL', 'ADA']  # These are missing data
        results = {}

        for symbol in missing_symbols:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {symbol}")
            logger.info(f"{'='*50}")

            try:
                success = self.fetch_and_save_missing_data(symbol)
                results[symbol] = success
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                results[symbol] = False

        return results

def main():
    """Main execution function"""
    logger.info("Starting multi-API data fetching for missing crypto data")
    logger.info("="*70)

    fetcher = MultiAPIDataFetcher()

    try:
        # Fetch missing data for all symbols
        results = fetcher.fetch_all_missing_data()

        # Print summary
        print("\n" + "="*70)
        print("MULTI-API DATA FETCHING SUMMARY")
        print("="*70)

        for symbol, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"{symbol}: {status}")

        print("\n" + "="*70)
        print("Data fetching completed!")
        print("You can now run: python update_correlated_groups_db.py")
        print("to update the database with the new data.")
        print("="*70)

    except Exception as e:
        logger.error(f"Error during data fetching: {e}")
        raise

if __name__ == "__main__":
    main()
