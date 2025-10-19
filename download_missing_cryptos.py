#!/usr/bin/env python3
"""
Download Missing Cryptocurrency Data

This script specifically downloads data for cryptocurrencies that were not
available in the top 50 list: FIL, ETC, THETA

Uses multiple data sources and APIs to try to get historical data.

Usage:
    python download_missing_cryptos.py

Output:
    - Adds data to the appropriate aligned folders
    - Updates organization structure if successful

Author: MVP Crypto Data Factory
Created: 2025-10-18
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

# Import APIs
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
    BINANCE_AVAILABLE = True
except ImportError:
    BINANCE_AVAILABLE = False
    print("Warning: python-binance not available")

try:
    import yfinance as yf
    YAHOO_AVAILABLE = True
except ImportError:
    YAHOO_AVAILABLE = False
    print("Warning: yfinance not available")

# Load environment variables
try:
    import os
    from dotenv import load_dotenv
    load_dotenv('/Users/mohamedcoulibaly/MVP/config.env')
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/missing_cryptos_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MissingCryptoDownloader:
    """Download data for missing cryptocurrencies."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.aligned_dir = self.base_dir / "data" / "aligned_by_period"
        self.logs_dir = self.base_dir / "logs"

        # Create directories
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Missing cryptocurrencies to try
        self.missing_cryptos = ['FIL', 'ETC', 'THETA']

        # Configuration
        self.config = {
            'START_DATE': '2020-01-01',
            'END_DATE': datetime.now().strftime('%Y-%m-%d'),
            'TIMEFRAME': '1h',
            'MAX_RETRIES': 3,
            'RETRY_DELAY': 5,
            'RATE_LIMIT_DELAY': 1,
        }

        # API clients
        self.binance_client = None
        if BINANCE_AVAILABLE:
            try:
                self.binance_client = Client(
                    api_key=os.getenv('BINANCE_API_KEY'),
                    api_secret=os.getenv('BINANCE_SECRET_KEY')
                )
                logger.info("Binance client initialized")
            except Exception as e:
                logger.warning(f"Could not initialize Binance client: {e}")

        logger.info(f"Missing crypto downloader initialized for: {self.missing_cryptos}")

    def check_crypto_availability(self, symbol: str) -> Dict[str, bool]:
        """Check if a cryptocurrency is available on different platforms."""
        availability = {
            'binance_spot': False,
            'binance_futures': False,
            'yahoo_finance': False,
            'coingecko': False
        }

        # Check Binance Spot
        if BINANCE_AVAILABLE and self.binance_client:
            try:
                # Try to get exchange info for the symbol
                exchange_info = self.binance_client.get_exchange_info()
                symbols_list = [s['symbol'] for s in exchange_info['symbols']]
                availability['binance_spot'] = f"{symbol}USDT" in symbols_list
            except Exception as e:
                logger.debug(f"Could not check Binance spot for {symbol}: {e}")

            try:
                # Check futures
                futures_exchange_info = self.binance_client.futures_exchange_info()
                futures_symbols = [s['symbol'] for s in futures_exchange_info['symbols']]
                availability['binance_futures'] = f"{symbol}USDT" in futures_symbols
            except Exception as e:
                logger.debug(f"Could not check Binance futures for {symbol}: {e}")

        # Check Yahoo Finance
        if YAHOO_AVAILABLE:
            try:
                ticker = yf.Ticker(f"{symbol}-USD")
                history = ticker.history(period="1d")
                availability['yahoo_finance'] = not history.empty
            except Exception as e:
                logger.debug(f"Could not check Yahoo Finance for {symbol}: {e}")

        # Check CoinGecko
        try:
            # Map symbols to CoinGecko IDs
            coingecko_ids = {
                'FIL': 'filecoin',
                'ETC': 'ethereum-classic',
                'THETA': 'theta-token'
            }

            cg_id = coingecko_ids.get(symbol)
            if cg_id:
                url = f"https://api.coingecko.com/api/v3/coins/{cg_id}"
                response = requests.get(url, timeout=10)
                availability['coingecko'] = response.status_code == 200
        except Exception as e:
            logger.debug(f"Could not check CoinGecko for {symbol}: {e}")

        return availability

    def fetch_binance_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch data from Binance for missing cryptocurrencies."""
        if not BINANCE_AVAILABLE or not self.binance_client:
            return None

        try:
            binance_symbol = f"{symbol}USDT"
            logger.info(f"Fetching Binance data for {symbol} ({binance_symbol})")

            # Get klines data
            klines = self.binance_client.get_historical_klines(
                symbol=binance_symbol,
                interval=Client.KLINE_INTERVAL_1HOUR,
                start_str=start_date,
                end_str=end_date
            )

            if not klines:
                logger.warning(f"No klines data returned from Binance for {symbol}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
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
            df['symbol'] = symbol
            df['name'] = symbol  # Will be updated with proper name if available
            df['market_cap_rank'] = 999  # Unknown rank
            df['timeframe'] = '1h'

            logger.info(f"Successfully fetched {len(df)} rows from Binance for {symbol}")
            return df

        except BinanceAPIException as e:
            if e.code == -1121:
                logger.warning(f"Symbol {symbol}USDT not available on Binance")
            else:
                logger.error(f"Binance API error for {symbol}: {e}")
        except Exception as e:
            logger.error(f"Error fetching from Binance for {symbol}: {e}")

        return None

    def fetch_yahoo_finance_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch data from Yahoo Finance for missing cryptocurrencies."""
        if not YAHOO_AVAILABLE:
            return None

        try:
            yahoo_symbol = f"{symbol}-USD"
            logger.info(f"Fetching Yahoo Finance data for {symbol} ({yahoo_symbol})")

            # Download data
            df = yf.download(
                yahoo_symbol,
                start=start_date,
                end=end_date,
                interval='1h',
                progress=False,
                prepost=False
            )

            if df.empty:
                logger.warning(f"No data returned from Yahoo Finance for {symbol}")
                return None

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

            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['symbol'] = symbol
            df['name'] = symbol
            df['market_cap_rank'] = 999
            df['timeframe'] = '1h'

            # Keep only required columns
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'name', 'market_cap_rank', 'timeframe']]

            logger.info(f"Successfully fetched {len(df)} rows from Yahoo Finance for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Error fetching from Yahoo Finance for {symbol}: {e}")
            return None

    def fetch_coingecko_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch data from CoinGecko for missing cryptocurrencies."""
        try:
            # Map symbols to CoinGecko IDs
            coingecko_ids = {
                'FIL': 'filecoin',
                'ETC': 'ethereum-classic',
                'THETA': 'theta-token'
            }

            cg_id = coingecko_ids.get(symbol)
            if not cg_id:
                logger.warning(f"No CoinGecko ID mapping for {symbol}")
                return None

            logger.info(f"Fetching CoinGecko data for {symbol} ({cg_id})")

            # CoinGecko market chart endpoint
            url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart"

            start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

            params = {
                'vs_currency': 'usd',
                'days': min(365, (end_ts - start_ts) // (24 * 3600)),  # Limit to 365 days
                'interval': 'hourly' if (end_ts - start_ts) <= (90 * 24 * 3600) else 'daily'
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if 'prices' not in data:
                logger.error(f"No price data found in CoinGecko response for {symbol}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(data['prices'], columns=['timestamp', 'close'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Add OHLC data (CoinGecko provides close prices)
            df['open'] = df['close']
            df['high'] = df['close']
            df['low'] = df['close']
            df['volume'] = 0  # Volume not available in free tier

            # Add metadata columns
            df['symbol'] = symbol
            df['name'] = symbol
            df['market_cap_rank'] = 999
            df['timeframe'] = '1h'

            # Keep required columns
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'name', 'market_cap_rank', 'timeframe']]

            logger.info(f"Successfully fetched {len(df)} rows from CoinGecko for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Error fetching from CoinGecko for {symbol}: {e}")
            return None

    def download_crypto_data(self, symbol: str) -> Optional[Dict]:
        """Download data for a specific cryptocurrency using multiple sources."""
        logger.info(f"\n{'='*50}")
        logger.info(f"Downloading data for {symbol}")
        logger.info(f"{'='*50}")

        # Check availability first
        availability = self.check_crypto_availability(symbol)
        logger.info(f"Platform availability for {symbol}: {availability}")

        # Try different data sources in order of preference
        data_sources = [
            ('Binance', lambda: self.fetch_binance_data(symbol, self.config['START_DATE'], self.config['END_DATE'])),
            ('Yahoo Finance', lambda: self.fetch_yahoo_finance_data(symbol, self.config['START_DATE'], self.config['END_DATE'])),
            ('CoinGecko', lambda: self.fetch_coingecko_data(symbol, self.config['START_DATE'], self.config['END_DATE']))
        ]

        for source_name, fetch_function in data_sources:
            logger.info(f"Trying {source_name} for {symbol}...")

            try:
                df = fetch_function()

                if df is not None and not df.empty and len(df) > 100:
                    logger.info(f"✅ Successfully downloaded {len(df)} rows from {source_name} for {symbol}")

                    # Validate and save the data
                    success = self.save_and_organize_data(symbol, df, source_name)

                    if success:
                        return {
                            'symbol': symbol,
                            'success': True,
                            'source': source_name,
                            'data_points': len(df),
                            'start_date': df['timestamp'].min().strftime('%Y-%m-%d'),
                            'end_date': df['timestamp'].max().strftime('%Y-%m-%d'),
                            'availability': availability
                        }

                else:
                    logger.warning(f"❌ {source_name} returned insufficient data for {symbol}")

            except Exception as e:
                logger.error(f"Error with {source_name} for {symbol}: {e}")

            # Rate limiting
            time.sleep(self.config['RATE_LIMIT_DELAY'])

        logger.error(f"❌ Failed to download data for {symbol} from any source")
        return {
            'symbol': symbol,
            'success': False,
            'source': 'NONE',
            'data_points': 0,
            'availability': availability
        }

    def save_and_organize_data(self, symbol: str, df: pd.DataFrame, source: str) -> bool:
        """Save and organize the downloaded data."""
        try:
            # Determine appropriate time period folder
            days_of_data = (df['timestamp'].max() - df['timestamp'].min()).days

            if days_of_data >= 2000:
                target_period = "2020-2025_full_history"
            elif days_of_data >= 1200:
                target_period = "2021-2025_established"
            elif days_of_data >= 800:
                target_period = "2023-2025_mid_term"
            elif days_of_data >= 300:
                target_period = "2024-2025_recent"
            elif days_of_data >= 180:
                target_period = "moderate_coverage"
            else:
                target_period = "limited_data"

            # Create target directory
            target_dir = self.aligned_dir / target_period / symbol.lower()
            target_dir.mkdir(parents=True, exist_ok=True)

            # Save hourly data
            csv_file = target_dir / f"{symbol.lower()}_hourly.csv"
            df.to_csv(csv_file, index=False)

            # Create metadata
            metadata = {
                'symbol': symbol,
                'name': symbol,
                'coingecko_id': symbol.lower(),
                'market_cap_rank': 999,
                'data_points': len(df),
                'start_date': df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S'),
                'end_date': df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S'),
                'downloaded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'download_source': source,
                'timeframe': '1h',
                'filename': f"{symbol.lower()}_hourly.csv"
            }

            metadata_file = target_dir / f"{symbol.lower()}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"✅ Saved {symbol} data to {target_period} folder ({len(df)} rows)")
            return True

        except Exception as e:
            logger.error(f"Error saving data for {symbol}: {e}")
            return False

    def download_all_missing_cryptos(self) -> Dict[str, Dict]:
        """Download data for all missing cryptocurrencies."""
        results = {}

        print("🔍 Downloading Missing Cryptocurrency Data")
        print("=" * 60)
        print(f"📊 Target Cryptocurrencies: {len(self.missing_cryptos)}")
        print(f"📁 Output: {self.aligned_dir}")
        print("=" * 60)

        for symbol in self.missing_cryptos:
            result = self.download_crypto_data(symbol)
            results[symbol] = result

            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            source = result.get('source', 'NONE')
            points = result.get('data_points', 0)
            print(f"{symbol}: {status} | Source: {source} | Points: {points:,}")

        return results

    def create_download_report(self, results: Dict[str, Dict]) -> None:
        """Create a comprehensive report of the download attempt."""
        successful_downloads = sum(1 for result in results.values() if result['success'])
        total_data_points = sum(result.get('data_points', 0) for result in results.values() if result['success'])

        report = {
            'download_attempt_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'target_cryptocurrencies': self.missing_cryptos,
            'summary': {
                'total_attempted': len(results),
                'successful': successful_downloads,
                'failed': len(results) - successful_downloads,
                'success_rate': f"{successful_downloads/len(results)*100:.1f}%" if results else "0%",
                'total_data_points': total_data_points
            },
            'results': results
        }

        # Save report
        report_file = self.base_dir / "data" / "missing_cryptos_download_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        # Print report
        print("\n" + "="*70)
        print("📊 MISSING CRYPTOCURRENCIES DOWNLOAD REPORT")
        print("="*70)
        print(f"Total Attempted: {report['summary']['total_attempted']}")
        print(f"Successful Downloads: {report['summary']['successful']}")
        print(f"Failed Downloads: {report['summary']['failed']}")
        print(f"Success Rate: {report['summary']['success_rate']}")
        print(f"Total Data Points: {report['summary']['total_data_points']:,}")
        print()

        if successful_downloads > 0:
            print("✅ SUCCESSFULLY DOWNLOADED:")
            for symbol, result in results.items():
                if result['success']:
                    source = result.get('source', 'UNKNOWN')
                    points = result.get('data_points', 0)
                    start_date = result.get('start_date', 'UNKNOWN')
                    print(f"   • {symbol}: {points:,} points from {source} (starts {start_date})")
            print()

        failed_cryptos = [symbol for symbol, result in results.items() if not result['success']]
        if failed_cryptos:
            print("❌ FAILED TO DOWNLOAD:")
            for symbol in failed_cryptos:
                availability = results[symbol].get('availability', {})
                available_platforms = [platform for platform, avail in availability.items() if avail]
                if available_platforms:
                    print(f"   • {symbol}: Available on {', '.join(available_platforms)} but data fetch failed")
                else:
                    print(f"   • {symbol}: Not available on any checked platforms")
            print()

        print("💡 RECOMMENDATIONS:")
        if successful_downloads > 0:
            print("   • Successfully downloaded cryptocurrencies have been organized into appropriate time period folders")
            print("   • Consider downloading 5-minute data for these symbols using the main downloader")
        if failed_cryptos:
            print("   • Failed cryptocurrencies may require premium data sources or manual data collection")
            print("   • Consider alternative exchanges or specialized crypto data providers")

        print("="*70)
        print(f"📄 Detailed report saved to: {report_file}")

def main():
    """Main execution function."""
    try:
        downloader = MissingCryptoDownloader()
        results = downloader.download_all_missing_cryptos()
        downloader.create_download_report(results)

    except Exception as e:
        logger.error(f"Error during missing cryptocurrencies download: {e}")
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()




