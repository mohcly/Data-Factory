#!/usr/bin/env python3
"""
Download 5-Minute Data for Full History Cryptocurrencies

This script downloads 5-minute interval OHLCV data for all cryptocurrencies
in the "2020-2025_full_history" folder using multiple APIs with fallback.

Features:
- Downloads 5-minute data (much more granular than hourly)
- Multi-API fallback (Binance primary, CoinGecko secondary)
- Progress tracking and error handling
- Organized output structure
- Uses your Binance API keys for enhanced rate limits

Usage:
    python download_5min_full_history.py

Configuration:
    - TIMEFRAME: '5m' (5-minute intervals)
    - START_DATE: '2020-01-01' (same as full history period)
    - END_DATE: Current date
    - APIS: Binance (primary), CoinGecko (fallback)

Output:
    - data/5min_full_history/{symbol}/{symbol}_5min.csv
    - data/5min_full_history/{symbol}/{symbol}_5min_metadata.json

Author: MVP Crypto Data Factory
Created: 2025-10-17
"""

import pandas as pd
import requests
import time
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json
from tqdm import tqdm
import numpy as np

# Import APIs
try:
    from binance.client import Client
    from dotenv import load_dotenv
    import os
    BINANCE_AVAILABLE = True
except ImportError:
    BINANCE_AVAILABLE = False
    print("Warning: python-binance not available")

# Load environment variables (your Binance API keys)
load_dotenv('/Users/mohamedcoulibaly/MVP/config.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/5min_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FiveMinDataDownloader:
    """Download 5-minute data for full history cryptocurrencies."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.output_dir = self.base_dir / "data" / "5min_full_history"
        self.logs_dir = self.base_dir / "logs"

        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Configuration
        self.config = {
            'TIMEFRAME': '5m',  # 5-minute data
            'START_DATE': '2020-01-01',
            'END_DATE': datetime.now().strftime('%Y-%m-%d'),
            'CHUNK_DAYS': 30,  # Smaller chunks for 5min data (API limits)
            'MAX_RETRIES': 3,
            'RETRY_DELAY': 5,
            'RATE_LIMIT_DELAY': 0.5,  # Faster for 5min data
        }

        # Load full history cryptocurrencies from master index
        self.full_history_cryptos = self.load_full_history_list()

        # API configurations
        self.apis = {
            'binance': {
                'name': 'Binance',
                'client': Client(
                    api_key=os.getenv('BINANCE_API_KEY'),
                    api_secret=os.getenv('BINANCE_SECRET_KEY')
                ) if BINANCE_AVAILABLE else None,
                'rate_limit': 1200,
                'free_tier': False,  # Using API keys
            },
            'coingecko': {
                'name': 'CoinGecko',
                'base_url': 'https://api.coingecko.com/api/v3',
                'rate_limit': 10,
                'free_tier': True,
            }
        }

        logger.info(f"Initialized 5min downloader for {len(self.full_history_cryptos)} cryptocurrencies")

    def load_full_history_list(self) -> List[str]:
        """Load the list of cryptocurrencies from the full history folder."""
        master_index_file = self.base_dir / "data" / "aligned_by_period" / "master_index.json"

        try:
            with open(master_index_file, 'r') as f:
                master_index = json.load(f)

            full_history_data = master_index['periods'].get('2020-2025_full_history', {})
            cryptocurrencies = full_history_data.get('cryptocurrencies', [])

            logger.info(f"Loaded {len(cryptocurrencies)} cryptocurrencies from full history: {cryptocurrencies}")
            return cryptocurrencies

        except Exception as e:
            logger.error(f"Error loading full history list: {e}")
            # Fallback to hardcoded list if master index fails
            fallback_list = ["BTC", "ETH", "BNB", "XRP", "ADA", "TRX", "DOGE", "LINK",
                           "XLM", "BCH", "LTC", "HBAR", "USDC", "ZEC"]
            logger.warning(f"Using fallback list: {fallback_list}")
            return fallback_list

    def fetch_binance_5min_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch 5-minute data from Binance API."""
        if not BINANCE_AVAILABLE:
            return pd.DataFrame()

        try:
            client = self.apis['binance']['client']
            binance_symbol = f"{symbol}USDT"

            logger.info(f"Fetching 5min {symbol} data from Binance: {start_date} to {end_date}")

            # For 5-minute data, we need to be more careful with date ranges
            # Binance has limits on historical data requests
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            # Calculate total days and create chunks
            total_days = (end_dt - start_dt).days
            chunk_size = min(self.config['CHUNK_DAYS'], total_days)

            all_klines = []
            current_start = start_dt

            with tqdm(total=total_days, desc=f"Binance {symbol} 5min", unit="days") as pbar:
                while current_start < end_dt:
                    current_end = min(current_start + timedelta(days=chunk_size), end_dt)

                    try:
                        # Get klines data
                        klines = client.get_historical_klines(
                            symbol=binance_symbol,
                            interval=Client.KLINE_INTERVAL_5MINUTE,
                            start_str=current_start.strftime("%Y-%m-%d"),
                            end_str=current_end.strftime("%Y-%m-%d")
                        )

                        if klines:
                            all_klines.extend(klines)
                            logger.info(f"Fetched {len(klines)} 5min candles for {symbol} chunk")

                        # Update progress
                        days_processed = (current_end - current_start).days
                        pbar.update(days_processed)

                        # Move to next chunk
                        current_start = current_end

                        # Rate limiting
                        time.sleep(self.config['RATE_LIMIT_DELAY'])

                    except Exception as chunk_error:
                        logger.warning(f"Error in chunk {current_start.date()} for {symbol}: {chunk_error}")
                        current_start = current_end
                        continue

            if not all_klines:
                logger.warning(f"No 5min klines data returned from Binance for {symbol}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(all_klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])

            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Convert string values to float
            for col in ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume',
                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Keep only essential columns
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

            # Remove duplicates and sort
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')

            logger.info(f"Successfully fetched {len(df)} 5min rows from Binance for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Error fetching 5min data from Binance for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_coingecko_5min_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch 5-minute data from CoinGecko API (limited free tier support)."""
        try:
            logger.info(f"Fetching 5min {symbol} data from CoinGecko: {start_date} to {end_date}")

            # CoinGecko free tier doesn't support 5-minute data well
            # We'll fetch daily data and interpolate, but this is not ideal for 5min
            logger.warning("CoinGecko free tier doesn't support 5-minute data. Using daily interpolation (not recommended for 5min analysis)")

            # Map symbols to CoinGecko IDs
            coingecko_ids = {
                'BTC': 'bitcoin', 'ETH': 'ethereum', 'BNB': 'binancecoin',
                'XRP': 'ripple', 'ADA': 'cardano', 'TRX': 'tron',
                'DOGE': 'dogecoin', 'LINK': 'chainlink', 'XLM': 'stellar',
                'BCH': 'bitcoin-cash', 'LTC': 'litecoin', 'HBAR': 'hedera-hashgraph',
                'USDC': 'usd-coin', 'ZEC': 'zcash'
            }

            cg_id = coingecko_ids.get(symbol)
            if not cg_id:
                logger.error(f"CoinGecko ID mapping not found for {symbol}")
                return pd.DataFrame()

            # Fetch daily data and interpolate to 5-minute (not accurate for analysis)
            url = f"{self.apis['coingecko']['base_url']}/coins/{cg_id}/market_chart"

            start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

            params = {
                'vs_currency': 'usd',
                'days': (end_ts - start_ts) // (24 * 3600),  # days
                'interval': 'daily'
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if 'prices' not in data:
                logger.error(f"No price data found in CoinGecko response for {symbol}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data['prices'], columns=['timestamp', 'close'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Add OHLC data (CoinGecko free tier only provides close prices)
            df['open'] = df['close']
            df['high'] = df['close']
            df['low'] = df['close']
            df['volume'] = np.nan

            # Interpolate to 5-minute intervals (WARNING: This creates artificial data points!)
            df = df.set_index('timestamp').resample('5T').interpolate(method='linear').reset_index()

            logger.warning(f"⚠️ Interpolated {len(df)} 5min rows from CoinGecko daily data for {symbol} (NOT RECOMMENDED)")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            logger.error(f"Error fetching 5min data from CoinGecko for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_crypto_5min_data(self, symbol: str) -> pd.DataFrame:
        """Fetch 5-minute data for a cryptocurrency using multiple APIs with fallback."""
        # API priority: Binance -> CoinGecko
        api_methods = [
            ('Binance', lambda: self.fetch_binance_5min_data(symbol, self.config['START_DATE'], self.config['END_DATE'])),
            ('CoinGecko', lambda: self.fetch_coingecko_5min_data(symbol, self.config['START_DATE'], self.config['END_DATE']))
        ]

        for api_name, method in api_methods:
            logger.info(f"Trying {api_name} for 5min {symbol}...")

            try:
                df = method()

                if not df.empty and len(df) > 1000:  # Require at least 1000 data points
                    quality = "HIGH" if api_name == "Binance" else "LOW (interpolated)"
                    logger.info(f"✅ Successfully fetched {len(df)} 5min rows from {api_name} for {symbol} (Quality: {quality})")

                    # Validate and clean data
                    df = self.validate_and_clean_data(df, symbol)

                    return df, api_name
                else:
                    logger.warning(f"⚠️ {api_name} returned insufficient data for {symbol}")

            except Exception as e:
                logger.error(f"❌ Error with {api_name} for 5min {symbol}: {e}")
                continue

            # Rate limiting delay
            time.sleep(self.config['RATE_LIMIT_DELAY'])

        logger.error(f"Failed to fetch 5min data for {symbol} from any API")
        return pd.DataFrame(), "FAILED"

    def validate_and_clean_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Validate and clean the downloaded 5-minute data."""
        try:
            # Remove duplicates
            df = df.drop_duplicates(subset=['timestamp'])

            # Sort by timestamp
            df = df.sort_values('timestamp')

            # Remove rows with all NaN values
            df = df.dropna(how='all')

            # Forward fill missing OHLC values
            ohlc_cols = ['open', 'high', 'low', 'close']
            df[ohlc_cols] = df[ohlc_cols].fillna(method='ffill')

            # Remove rows where close price is 0 or negative
            df = df[df['close'] > 0]

            # Reset index
            df = df.reset_index(drop=True)

            logger.info(f"Validated 5min data for {symbol}: {len(df)} rows after cleaning")
            return df

        except Exception as e:
            logger.error(f"Error validating 5min data for {symbol}: {e}")
            return df

    def save_crypto_5min_data(self, symbol: str, df: pd.DataFrame, api_source: str) -> bool:
        """Save 5-minute cryptocurrency data to file."""
        try:
            # Create symbol directory
            symbol_dir = self.output_dir / symbol.lower()
            symbol_dir.mkdir(exist_ok=True)

            # Save as CSV
            filename = f"{symbol.lower()}_5min.csv"
            filepath = symbol_dir / filename

            # Add metadata columns to the DataFrame
            df_copy = df.copy()
            df_copy['symbol'] = symbol
            df_copy['timeframe'] = '5m'
            df_copy['api_source'] = api_source

            # Save to CSV
            df_copy.to_csv(filepath, index=False)

            # Save metadata separately
            metadata = {
                'symbol': symbol,
                'name': symbol,  # Could be enhanced with full names
                'timeframe': '5min',
                'api_source': api_source,
                'data_points': len(df),
                'start_date': df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S') if not df.empty else None,
                'end_date': df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S') if not df.empty else None,
                'downloaded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'expected_interval': '5 minutes',
                'quality_note': 'HIGH' if api_source == 'Binance' else 'LOW (interpolated from daily)',
                'config': self.config
            }

            metadata_file = symbol_dir / f"{symbol.lower()}_5min_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"✅ Saved {len(df)} 5min rows for {symbol} to {filepath}")
            return True

        except Exception as e:
            logger.error(f"Error saving 5min data for {symbol}: {e}")
            return False

    def download_all_5min_data(self) -> Dict[str, Dict]:
        """Download 5-minute data for all full history cryptocurrencies."""
        results = {}

        print("🚀 Starting 5-Minute Data Download for Full History Cryptocurrencies")
        print("=" * 80)
        print(f"📊 Cryptocurrencies to process: {len(self.full_history_cryptos)}")
        print(f"📅 Time period: {self.config['START_DATE']} to {self.config['END_DATE']}")
        print(f"⏱️  Interval: {self.config['TIMEFRAME']}")
        print("=" * 80)

        # Create progress bar
        with tqdm(total=len(self.full_history_cryptos), desc="Downloading 5min Data", unit="crypto") as pbar:
            for symbol in self.full_history_cryptos:
                print(f"\n{'='*60}")
                print(f"Processing {symbol} 5-minute data...")
                print(f"{'='*60}")

                try:
                    # Fetch data
                    df, api_source = self.fetch_crypto_5min_data(symbol)

                    if not df.empty:
                        # Save data
                        success = self.save_crypto_5min_data(symbol, df, api_source)
                        results[symbol] = {
                            'success': success,
                            'api_source': api_source,
                            'data_points': len(df),
                            'start_date': df['timestamp'].min().strftime('%Y-%m-%d') if not df.empty else None,
                            'end_date': df['timestamp'].max().strftime('%Y-%m-%d') if not df.empty else None
                        }
                    else:
                        logger.error(f"No 5min data could be fetched for {symbol}")
                        results[symbol] = {
                            'success': False,
                            'api_source': 'FAILED',
                            'data_points': 0,
                            'error': 'No data available'
                        }

                except Exception as e:
                    logger.error(f"Error processing 5min data for {symbol}: {e}")
                    results[symbol] = {
                        'success': False,
                        'api_source': 'ERROR',
                        'data_points': 0,
                        'error': str(e)
                    }

                pbar.update(1)

        return results

    def create_download_summary(self, results: Dict[str, Dict]) -> None:
        """Create a summary of the 5-minute data download operation."""
        # Calculate statistics
        successful_downloads = sum(1 for result in results.values() if result['success'])
        total_data_points = sum(result['data_points'] for result in results.values() if result['success'])

        # Create summary
        summary = {
            'download_summary': {
                'total_cryptos': len(results),
                'successful': successful_downloads,
                'failed': len(results) - successful_downloads,
                'success_rate': f"{successful_downloads/len(results)*100:.1f}%" if results else "0%",
                'total_data_points': total_data_points,
                'avg_data_points_per_crypto': total_data_points / successful_downloads if successful_downloads > 0 else 0
            },
            'configuration': self.config,
            'results': results,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'api_sources_used': list(set(result['api_source'] for result in results.values() if result['success']))
        }

        # Save summary
        summary_file = self.output_dir / "5min_download_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        # Print summary
        print("\n" + "="*80)
        print("📊 5-MINUTE DATA DOWNLOAD SUMMARY")
        print("="*80)
        print(f"Total Cryptocurrencies: {summary['download_summary']['total_cryptos']}")
        print(f"Successful Downloads: {summary['download_summary']['successful']}")
        print(f"Failed Downloads: {summary['download_summary']['failed']}")
        print(f"Success Rate: {summary['download_summary']['success_rate']}")
        print(f"Total Data Points: {summary['download_summary']['total_data_points']:,}")
        print(f"Average Points/Crypto: {summary['download_summary']['avg_data_points_per_crypto']:,.0f}")
        print(f"API Sources Used: {', '.join(summary['api_sources_used'])}")
        print(f"Output Directory: {self.output_dir}")
        print("="*80)

        # List failed downloads
        failed_cryptos = [symbol for symbol, result in results.items() if not result['success']]
        if failed_cryptos:
            print("\n❌ Failed Downloads:")
            for symbol in failed_cryptos:
                error = results[symbol].get('error', 'Unknown error')
                print(f"   • {symbol}: {error}")

        print("\n✅ 5-minute data download operation completed!")

def main():
    """Main execution function."""
    downloader = FiveMinDataDownloader()

    try:
        # Download all 5-minute data
        results = downloader.download_all_5min_data()

        # Create summary report
        downloader.create_download_summary(results)

    except KeyboardInterrupt:
        logger.warning("5-minute data download interrupted by user")
        print("\n⚠️ Download interrupted by user")

    except Exception as e:
        logger.error(f"Error during 5-minute data download operation: {e}")
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()




