#!/usr/bin/env python3
"""
Fetch Liquidation Data for Full History Cryptocurrencies

This script fetches liquidation data from Binance Futures for the same cryptocurrencies
that have full historical data. Liquidation data shows forced position closures due to
insufficient margin, providing valuable market stress indicators.

Data Sources:
- Binance Futures API (primary)
- Alternative exchanges if needed

Features:
- Historical liquidation data collection
- Multiple timeframes support
- Data validation and cleaning
- Organized storage with existing datasets

Usage:
    python fetch_liquidation_data.py

Output:
    - data/liquidation_data/{symbol}/{symbol}_liquidations.csv
    - data/liquidation_data/{symbol}/{symbol}_liquidations_metadata.json

Author: MVP Crypto Data Factory
Created: 2025-10-18
"""

import pandas as pd
import requests
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

# Import Binance API
BINANCE_AVAILABLE = False
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
    BINANCE_AVAILABLE = True
except ImportError:
    print("Warning: python-binance not available")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/liquidation_data_fetch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables (Binance API keys)
try:
    import os
    from dotenv import load_dotenv
    load_dotenv('/Users/mohamedcoulibaly/MVP/config.env')
except ImportError:
    pass

class LiquidationDataFetcher:
    """Fetch liquidation data from cryptocurrency exchanges."""

    def __init__(self):
        self.base_dir = Path("/Users/mohamedcoulibaly/MVP/Crypto/Data-factory")
        self.output_dir = self.base_dir / "data" / "liquidation_data"
        self.logs_dir = self.base_dir / "logs"

        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Configuration
        self.config = {
            'START_DATE': '2020-01-01',
            'END_DATE': datetime.now().strftime('%Y-%m-%d'),
            'MAX_RETRIES': 3,
            'RETRY_DELAY': 5,
            'RATE_LIMIT_DELAY': 0.1,  # Binance rate limits are generous for this endpoint
            'BATCH_SIZE': 1000,  # Number of liquidation records per request
        }

        # Load full history cryptocurrencies
        self.full_history_cryptos = self.load_full_history_list()

        # API clients
        self.binance_client = None
        if BINANCE_AVAILABLE:
            try:
                self.binance_client = Client(
                    api_key=os.getenv('BINANCE_API_KEY'),
                    api_secret=os.getenv('BINANCE_SECRET_KEY')
                )
                logger.info("Binance Futures client initialized")
            except Exception as e:
                logger.warning(f"Could not initialize Binance client: {e}")
                self.binance_client = None

        logger.info(f"Liquidation data fetcher initialized for {len(self.full_history_cryptos)} cryptocurrencies")

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
            # Fallback to hardcoded list
            fallback_list = ["BTC", "ETH", "BNB", "XRP", "ADA", "TRX", "DOGE", "LINK",
                           "XLM", "BCH", "LTC", "HBAR", "USDC", "ZEC"]
            logger.warning(f"Using fallback list: {fallback_list}")
            return fallback_list

    def get_binance_futures_liquidations(self, symbol: str, start_time: int, end_time: int) -> List[Dict]:
        """Fetch liquidation data from Binance Futures."""
        if not BINANCE_AVAILABLE or not self.binance_client:
            return []

        try:
            # Binance Futures liquidation endpoint
            # Note: Binance doesn't have a direct historical liquidation API
            # We'll need to use alternative approaches

            # For now, let's check if we can get recent liquidation data
            # Binance Futures API has some liquidation endpoints but they may be limited

            logger.info(f"Checking Binance Futures liquidation data availability for {symbol}")

            # Try to get recent liquidations (last 24 hours as example)
            try:
                # This endpoint might not exist or be restricted
                liquidations = self.binance_client.futures_liquidation_orders(
                    symbol=f"{symbol}USDT",
                    startTime=start_time,
                    endTime=end_time,
                    limit=1000
                )

                if liquidations:
                    logger.info(f"Found {len(liquidations)} liquidations for {symbol}")
                    return liquidations
                else:
                    logger.info(f"No liquidations found for {symbol} in the specified period")

            except BinanceAPIException as e:
                if e.code == -1121:
                    logger.warning(f"Symbol {symbol}USDT not available on Binance Futures")
                else:
                    logger.error(f"Binance API error for {symbol}: {e}")
            except Exception as e:
                logger.error(f"Error fetching liquidations for {symbol}: {e}")

        except Exception as e:
            logger.error(f"Error in Binance Futures liquidation fetch for {symbol}: {e}")

        return []

    def get_recent_liquidations_binance(self, symbol: str, hours: int = 24) -> List[Dict]:
        """Get recent liquidation data from Binance Futures (last N hours)."""
        if not BINANCE_AVAILABLE or not self.binance_client:
            return []

        try:
            # Calculate time range
            end_time = int(datetime.now().timestamp() * 1000)
            start_time = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)

            logger.info(f"Fetching recent {hours}h liquidations for {symbol}")

            # Try different approaches for liquidation data
            liquidations = []

            # Method 1: Try futures liquidation orders
            try:
                orders = self.binance_client.futures_liquidation_orders(
                    symbol=f"{symbol}USDT",
                    startTime=start_time,
                    endTime=end_time,
                    limit=1000
                )
                if orders:
                    liquidations.extend(orders)
                    logger.info(f"Found {len(orders)} liquidation orders for {symbol}")
            except Exception as e:
                logger.debug(f"Could not fetch liquidation orders for {symbol}: {e}")

            # Method 2: Try force liquidation orders (alternative endpoint)
            try:
                force_orders = self.binance_client.futures_force_liquidation_orders(
                    symbol=f"{symbol}USDT",
                    startTime=start_time,
                    endTime=end_time,
                    limit=1000
                )
                if force_orders:
                    liquidations.extend(force_orders)
                    logger.info(f"Found {len(force_orders)} force liquidation orders for {symbol}")
            except Exception as e:
                logger.debug(f"Could not fetch force liquidation orders for {symbol}: {e}")

            return liquidations

        except Exception as e:
            logger.error(f"Error fetching recent liquidations for {symbol}: {e}")
            return []

    def check_liquidation_data_availability(self) -> Dict[str, Dict]:
        """Check which cryptocurrencies have liquidation data available."""
        availability = {}

        if not BINANCE_AVAILABLE or not self.binance_client:
            logger.error("Binance API not available. Cannot check liquidation data.")
            for symbol in self.full_history_cryptos:
                availability[symbol] = {
                    'available': False,
                    'sample_count': 0,
                    'last_checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'error': 'Binance API not available'
                }
            return availability

        logger.info("🔍 Checking liquidation data availability...")

        for symbol in self.full_history_cryptos:
            logger.info(f"Checking {symbol}...")

            # Try to get recent 24h liquidations
            liquidations = self.get_recent_liquidations_binance(symbol, hours=24)

            availability[symbol] = {
                'available': len(liquidations) > 0,
                'sample_count': len(liquidations),
                'last_checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_source': 'binance_futures' if liquidations else None,
                'sample_data': liquidations[:3] if liquidations else []  # First 3 records as sample
            }

            if liquidations:
                logger.info(f"✅ {symbol}: {len(liquidations)} liquidations found (24h)")
            else:
                logger.warning(f"❌ {symbol}: No liquidation data available")

            # Rate limiting
            time.sleep(self.config['RATE_LIMIT_DELAY'])

        return availability

    def create_availability_report(self, availability: Dict[str, Dict]) -> None:
        """Create a comprehensive report on liquidation data availability."""
        report = {
            'assessment_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_cryptos_checked': len(availability),
            'availability_summary': {
                'available': sum(1 for info in availability.values() if info['available']),
                'not_available': sum(1 for info in availability.values() if not info['available']),
                'total_liquidations_sampled': sum(info['sample_count'] for info in availability.values())
            },
            'available_cryptos': [symbol for symbol, info in availability.items() if info['available']],
            'unavailable_cryptos': [symbol for symbol, info in availability.items() if not info['available']],
            'detailed_results': availability
        }

        # Save report
        report_file = self.output_dir / "liquidation_availability_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        # Print summary
        print("\n" + "="*70)
        print("🔍 LIQUIDATION DATA AVAILABILITY ASSESSMENT")
        print("="*70)
        print(f"Total Cryptocurrencies Checked: {report['total_cryptos_checked']}")
        print(f"Available: {report['availability_summary']['available']}")
        print(f"Not Available: {report['availability_summary']['not_available']}")
        print(f"Total Sample Liquidations: {report['availability_summary']['total_liquidations_sampled']}")
        print()

        if report['available_cryptos']:
            print("✅ AVAILABLE CRYPTOCURRENCIES:")
            for symbol in report['available_cryptos']:
                count = availability[symbol]['sample_count']
                print(f"   • {symbol}: {count} liquidations (24h sample)")
            print()

        if report['unavailable_cryptos']:
            print("❌ NOT AVAILABLE CRYPTOCURRENCIES:")
            for symbol in report['unavailable_cryptos'][:10]:  # Show first 10
                print(f"   • {symbol}")
            if len(report['unavailable_cryptos']) > 10:
                print(f"   ... and {len(report['unavailable_cryptos']) - 10} more")
            print()

        print("💡 RECOMMENDATIONS:")
        if report['availability_summary']['available'] > 0:
            print("   • Liquidation data is available for some cryptocurrencies")
            print("   • Consider implementing historical liquidation data collection")
            print("   • Useful for market stress analysis and capitulation signals")
        else:
            print("   • No liquidation data available through current APIs")
            print("   • Consider alternative data sources or premium APIs")
            print("   • May need to implement real-time liquidation monitoring")

        print("="*70)
        print(f"📄 Detailed report saved to: {report_file}")

    def suggest_alternative_approaches(self) -> None:
        """Suggest alternative approaches for obtaining liquidation data."""
        print("\n" + "="*60)
        print("💡 ALTERNATIVE APPROACHES FOR LIQUIDATION DATA")
        print("="*60)

        alternatives = [
            {
                'name': 'Real-time WebSocket Monitoring',
                'description': 'Monitor Binance Futures WebSocket streams for live liquidations',
                'pros': ['Real-time data', 'Complete coverage', 'No historical gaps'],
                'cons': ['Requires continuous operation', 'Only future data', 'Resource intensive'],
                'implementation': 'Use Binance WebSocket API with liquidation stream'
            },
            {
                'name': 'Premium Data Providers',
                'description': 'Use specialized crypto data providers (e.g., CryptoCompare, CoinAPI)',
                'pros': ['Historical data available', 'Multiple exchanges', 'High quality'],
                'cons': ['Subscription costs', 'API limits', 'May not be comprehensive'],
                'implementation': 'Subscribe to premium crypto data service'
            },
            {
                'name': 'Alternative Exchanges',
                'description': 'Fetch from exchanges with better liquidation APIs (Bybit, OKX, etc.)',
                'pros': ['Different market perspectives', 'May have better APIs', 'Additional data'],
                'cons': ['Different market dynamics', 'API complexity', 'Rate limits'],
                'implementation': 'Implement multi-exchange liquidation data collection'
            },
            {
                'name': 'Community Data Sources',
                'description': 'Use aggregated liquidation data from crypto analytics platforms',
                'pros': ['Comprehensive coverage', 'Community maintained', 'Free/low cost'],
                'cons': ['Data quality varies', 'Potential delays', 'May be incomplete'],
                'implementation': 'Integrate with liquidation tracking services'
            },
            {
                'name': 'On-chain Analysis',
                'description': 'Analyze liquidation events through blockchain transaction monitoring',
                'pros': ['Most accurate', 'Real-time', 'Comprehensive'],
                'cons': ['Very complex implementation', 'High technical requirements', 'Resource intensive'],
                'implementation': 'Build custom blockchain scanner for liquidation transactions'
            }
        ]

        for alt in alternatives:
            print(f"\n🔧 {alt['name']}")
            print(f"   {alt['description']}")
            print(f"   ✅ Pros: {', '.join(alt['pros'])}")
            print(f"   ❌ Cons: {', '.join(alt['cons'])}")
            print(f"   🛠️  Implementation: {alt['implementation']}")

        print("\n" + "="*60)

def main():
    """Main execution function."""
    print("🔍 Checking Liquidation Data Availability")
    print("=" * 50)

    fetcher = LiquidationDataFetcher()

    try:
        # Check availability
        availability = fetcher.check_liquidation_data_availability()

        # Create report
        fetcher.create_availability_report(availability)

        # Suggest alternatives
        fetcher.suggest_alternative_approaches()

    except Exception as e:
        logger.error(f"Error during liquidation data assessment: {e}")
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
