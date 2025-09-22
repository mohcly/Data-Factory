"""
Data Fetching Bot - Main Application
Orchestrates database management, API clients, and data processing
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .database.database_manager import DatabaseManager
from .api.binance_client import BinanceAPIClient
from .api.coingecko_client import CoinGeckoAPIClient
from .data_processing.data_validator import DataValidator

logger = logging.getLogger(__name__)


class DataFetchingBot:
    """Main data fetching bot application"""

    def __init__(self, db_path: str = None, config: Dict = None):
        """Initialize the data fetching bot"""
        self.config = config or {}
        self.db_manager = DatabaseManager(db_path)
        self.data_validator = DataValidator()
        self.api_clients = []

        # Initialize API clients
        self._initialize_api_clients()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.running = False
        logger.info("Data Fetching Bot initialized successfully")

    def _initialize_api_clients(self):
        """Initialize all available API clients"""
        # Initialize Binance client
        try:
            binance_client = BinanceAPIClient()
            self.api_clients.append(binance_client)
            logger.info("✅ Binance API client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Binance client: {e}")

        # Initialize CoinGecko client
        try:
            coingecko_client = CoinGeckoAPIClient()
            self.api_clients.append(coingecko_client)
            logger.info("✅ CoinGecko API client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize CoinGecko client: {e}")

        if not self.api_clients:
            raise Exception("No API clients could be initialized")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False

    async def start(self):
        """Start the data fetching bot"""
        logger.info("🚀 Starting Data Fetching Bot...")

        try:
            self.running = True

            # Test API connectivity
            await self._test_api_connectivity()

            # Get supported symbols
            symbols = self._get_supported_symbols()

            # Start continuous data fetching
            await self._start_continuous_fetching(symbols)

        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            self.running = False

    async def _test_api_connectivity(self):
        """Test connectivity to all API clients"""
        logger.info("🔍 Testing API connectivity...")

        for client in self.api_clients:
            try:
                if hasattr(client, 'ping'):
                    healthy = await client.ping()
                else:
                    # Perform a simple request test
                    await client.make_request('/ping')
                    healthy = True

                if healthy:
                    logger.info(f"✅ {client.name} API is healthy")
                else:
                    logger.warning(f"⚠️ {client.name} API connectivity test failed")

            except Exception as e:
                logger.error(f"❌ {client.name} API connectivity test failed: {e}")

    def _get_supported_symbols(self) -> List[str]:
        """Get list of supported cryptocurrency symbols"""
        default_symbols = self.db_manager.get_config_value('supported_symbols', 'BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT')
        return [symbol.strip() for symbol in default_symbols.split(',')]

    async def _start_continuous_fetching(self, symbols: List[str]):
        """Start continuous data fetching for all symbols"""
        logger.info(f"📊 Starting continuous data fetching for {len(symbols)} symbols: {symbols}")

        while self.running:
            try:
                # Fetch data for each symbol
                for symbol in symbols:
                    if not self.running:
                        break

                    await self._fetch_symbol_data(symbol)

                # Wait before next iteration
                interval_minutes = self.config.get('fetch_interval_minutes', 5)
                logger.info(f"⏰ Waiting {interval_minutes} minutes before next fetch cycle...")
                await asyncio.sleep(interval_minutes * 60)

            except Exception as e:
                logger.error(f"Error in fetch cycle: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying

    async def _fetch_symbol_data(self, symbol: str):
        """Fetch data for a single symbol from all available APIs"""
        logger.info(f"📈 Fetching data for {symbol}")

        fetched_data = []

        for client in self.api_clients:
            if not client.is_healthy():
                logger.warning(f"Skipping unhealthy API: {client.name}")
                continue

            try:
                # Fetch historical data (last 24 hours, hourly intervals)
                end_time = datetime.now()
                start_time = end_time - timedelta(days=1)

                if isinstance(client, BinanceAPIClient):
                    data = await client.get_klines(
                        symbol=symbol,
                        interval='1h',
                        start_time=start_time.isoformat(),
                        limit=24
                    )
                elif isinstance(client, CoinGeckoAPIClient):
                    # CoinGecko doesn't have direct OHLCV, so we'll use market chart
                    chart_data = await client.get_coin_market_chart(
                        coin_id=symbol.lower(),
                        vs_currency='usd',
                        days=1,
                        interval='hourly'
                    )

                    # Convert market chart data to OHLCV format
                    data = []
                    for point in chart_data.get('data_points', []):
                        # For simplicity, use same price for O/H/L/C since we don't have OHLCV
                        price = point.get('price', 0)
                        data.append({
                            'timestamp': point['timestamp'],
                            'open': price,
                            'high': price,
                            'low': price,
                            'close': price,
                            'volume': point.get('total_volume', 0)
                        })
                else:
                    logger.warning(f"No fetch method implemented for {client.name}")
                    continue

                # Validate and normalize data
                if data:
                    validated_data = self.data_validator.validate_ohlcv_data(data, client.name)
                    normalized_data = self.data_validator.normalize_data(data, symbol)

                    # Add quality score from validation
                    for record in normalized_data:
                        record['data_quality_score'] = validated_data.get('quality_score', 1.0)
                        record['is_validated'] = validated_data.get('is_valid', False)
                        record['source_api'] = client.name

                    fetched_data.extend(normalized_data)

                    # Update API performance metrics
                    self.db_manager.update_api_performance(
                        api_name=client.name,
                        symbol=symbol,
                        success=True,
                        response_time=client.get_performance_metrics()['average_response_time']
                    )

                    logger.info(f"✅ Fetched {len(data)} records for {symbol} from {client.name}")

                else:
                    logger.warning(f"⚠️ No data received for {symbol} from {client.name}")

            except Exception as e:
                logger.error(f"❌ Failed to fetch data for {symbol} from {client.name}: {e}")

                # Update API performance metrics for failure
                self.db_manager.update_api_performance(
                    api_name=client.name,
                    symbol=symbol,
                    success=False
                )

                # Log error in database
                self.db_manager.log_error(
                    error_type='api_fetch_error',
                    error_message=str(e),
                    component=f'{client.name}_client',
                    severity='error'
                )

        # Insert collected data into database
        if fetched_data:
            inserted_count = self.db_manager.insert_crypto_data(fetched_data)
            logger.info(f"💾 Inserted {inserted_count} records for {symbol} into database")

            # Update last fetch timestamp
            self.db_manager.set_config_value('last_data_fetch', datetime.now().isoformat())

    async def fetch_historical_data(self, symbol: str, start_date: str, end_date: str = None,
                                  interval: str = '1h') -> int:
        """Fetch historical data for backfilling"""
        logger.info(f"📜 Fetching historical data for {symbol} from {start_date} to {end_date or 'now'}")

        total_inserted = 0

        for client in self.api_clients:
            if not client.is_healthy():
                continue

            try:
                if isinstance(client, BinanceAPIClient):
                    # Fetch in batches to avoid API limits
                    data = await client.get_historical_klines_batch(
                        symbol=symbol,
                        interval=interval,
                        start_date=start_date,
                        end_date=end_date,
                        max_requests=100
                    )
                elif isinstance(client, CoinGeckoAPIClient):
                    # CoinGecko historical data
                    chart_data = await client.get_coin_market_chart(
                        coin_id=symbol.lower(),
                        vs_currency='usd',
                        days='max'
                    )
                    data = []
                    for point in chart_data.get('data_points', []):
                        price = point.get('price', 0)
                        data.append({
                            'timestamp': point['timestamp'],
                            'open': price,
                            'high': price,
                            'low': price,
                            'close': price,
                            'volume': point.get('total_volume', 0)
                        })
                else:
                    continue

                if data:
                    # Validate and normalize
                    validated_data = self.data_validator.validate_ohlcv_data(data, client.name)
                    normalized_data = self.data_validator.normalize_data(data, symbol)

                    for record in normalized_data:
                        record['data_quality_score'] = validated_data.get('quality_score', 1.0)
                        record['is_validated'] = validated_data.get('is_valid', False)
                        record['source_api'] = client.name

                    # Insert into database
                    inserted_count = self.db_manager.insert_crypto_data(normalized_data)
                    total_inserted += inserted_count

                    logger.info(f"✅ Inserted {inserted_count} historical records for {symbol} from {client.name}")

            except Exception as e:
                logger.error(f"❌ Failed to fetch historical data for {symbol} from {client.name}: {e}")

        return total_inserted

    def get_database_info(self) -> Dict:
        """Get comprehensive database information"""
        return self.db_manager.get_database_info()

    def get_api_performance(self) -> List[Dict]:
        """Get performance metrics for all API clients"""
        performance_data = []

        for client in self.api_clients:
            metrics = client.get_performance_metrics()
            performance_data.append(metrics)

        return performance_data

    async def stop(self):
        """Stop the data fetching bot"""
        logger.info("🛑 Stopping Data Fetching Bot...")
        self.running = False

        # Wait for any ongoing operations to complete
        await asyncio.sleep(2)

        logger.info("✅ Data Fetching Bot stopped successfully")


async def main():
    """Main entry point for the data fetching bot"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/workspace/memory-bank/data-fetching-bot/logs/bot.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger.info("Starting Data Fetching Bot...")

    # Create and start the bot
    bot = DataFetchingBot()

    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())