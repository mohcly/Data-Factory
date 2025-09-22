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
from .api.api_manager import APIManager
from .data_processing.data_validator import DataValidator
from .data_processing.gap_detector import GapDetector
from .data_processing.backfill_manager import BackfillManager
from .data_processing.parallel_processor import ParallelProcessor, ConcurrentDataFetcher
from .error_handling.retry_manager import RetryManager, RetryStrategy

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

        # Initialize API manager with failover and load balancing
        self.api_manager = APIManager(self.api_clients)
        self.api_manager.db_manager = self.db_manager  # For error logging

        # Initialize retry manager with exponential backoff
        self.retry_manager = RetryManager(
            max_retries=3,
            base_delay=1.0,
            max_delay=30.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            jitter=True
        )

        # Initialize gap detector for data integrity monitoring
        self.gap_detector = GapDetector(self.db_manager)

        # Initialize backfill manager for historical data recovery
        self.backfill_manager = BackfillManager(self.api_manager, self.db_manager, self.retry_manager)

        # Initialize parallel processor for concurrent operations
        self.parallel_processor = ParallelProcessor(max_workers=5, max_queue_size=1000)
        self.concurrent_fetcher = ConcurrentDataFetcher(self.parallel_processor)

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

            # Start parallel processor
            await self.parallel_processor.start()

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

        try:
            # Test API manager connectivity
            await self.api_manager.make_request('get_klines',
                                               symbol='BTCUSDT',
                                               interval='1h',
                                               limit=1)
            logger.info("✅ API Manager connectivity test passed")
        except Exception as e:
            logger.error(f"❌ API Manager connectivity test failed: {e}")

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
        """Fetch data for a single symbol using retry manager and API manager"""
        logger.info(f"📈 Fetching data for {symbol}")

        async def fetch_operation(**kwargs):
            """Wrapped fetch operation for retry manager"""
            return await self.api_manager.make_request('get_klines', **kwargs)

        try:
            # Fetch historical data (last 24 hours, hourly intervals) with retry logic
            end_time = datetime.now()
            start_time = end_time - timedelta(days=1)

            data = await self.retry_manager.execute_with_retry(
                fetch_operation,
                component=f"fetch_{symbol}",
                symbol=symbol,
                interval='1h',
                start_time=start_time.isoformat(),
                limit=24
            )

            # Validate and normalize data
            if data:
                validated_data = self.data_validator.validate_ohlcv_data(data, 'api_manager')
                normalized_data = self.data_validator.normalize_data(data, symbol)

                # Add quality score and source information
                for record in normalized_data:
                    record['data_quality_score'] = validated_data.get('quality_score', 1.0)
                    record['is_validated'] = validated_data.get('is_valid', False)
                    record['source_api'] = 'api_manager'

                # Insert collected data into database
                inserted_count = self.db_manager.insert_crypto_data(normalized_data)
                logger.info(f"✅ Fetched and inserted {len(data)} records for {symbol}")

                # Update last fetch timestamp
                self.db_manager.set_config_value('last_data_fetch', datetime.now().isoformat())

                return len(data)

            else:
                logger.warning(f"⚠️ No data received for {symbol}")
                return 0

        except Exception as e:
            logger.error(f"❌ Failed to fetch data for {symbol} after all retries: {e}")

            # Log error in database
            self.db_manager.log_error(
                error_type='api_fetch_error',
                error_message=str(e),
                component=f'fetch_{symbol}',
                severity='error'
            )

            return 0

    async def fetch_historical_data(self, symbol: str, start_date: str, end_date: str = None,
                                  interval: str = '1h') -> int:
        """Fetch historical data for backfilling using API manager"""
        logger.info(f"📜 Fetching historical data for {symbol} from {start_date} to {end_date or 'now'}")

        try:
            # Use API manager for historical data fetching
            data = await self.api_manager.make_request(
                'get_historical_klines_batch',
                symbol=symbol,
                interval=interval,
                start_date=start_date,
                end_date=end_date,
                max_requests=100
            )

            if data:
                # Validate and normalize
                validated_data = self.data_validator.validate_ohlcv_data(data, 'api_manager')
                normalized_data = self.data_validator.normalize_data(data, symbol)

                for record in normalized_data:
                    record['data_quality_score'] = validated_data.get('quality_score', 1.0)
                    record['is_validated'] = validated_data.get('is_valid', False)
                    record['source_api'] = 'api_manager'

                # Insert into database
                inserted_count = self.db_manager.insert_crypto_data(normalized_data)
                logger.info(f"✅ Inserted {inserted_count} historical records for {symbol}")

                return inserted_count
            else:
                logger.warning(f"⚠️ No historical data received for {symbol}")
                return 0

        except Exception as e:
            logger.error(f"❌ Failed to fetch historical data for {symbol}: {e}")
            return 0

    def get_database_info(self) -> Dict:
        """Get comprehensive database information"""
        return self.db_manager.get_database_info()

    def get_api_performance(self) -> List[Dict]:
        """Get performance metrics for all API clients via API manager"""
        performance_data = []

        # Get comprehensive performance report from API manager
        api_manager_report = self.api_manager.get_performance_report()

        # Get individual client metrics
        for client in self.api_clients:
            client_metrics = client.get_performance_metrics()
            api_manager_metrics = api_manager_report.get(client.name, {})

            # Combine metrics
            combined_metrics = {
                'client_name': client.name,
                'client_metrics': client_metrics,
                'api_manager_metrics': api_manager_metrics,
                'health_status': 'healthy' if client.is_healthy() else 'unhealthy'
            }

            performance_data.append(combined_metrics)

        return performance_data

    def get_api_manager_status(self) -> Dict:
        """Get comprehensive API manager status"""
        return {
            'client_status': self.api_manager.get_client_status(),
            'performance_report': self.api_manager.get_performance_report(),
            'total_clients': len(self.api_clients),
            'healthy_clients': len([c for c in self.api_clients if c.is_healthy()])
        }

    def reset_api_manager(self):
        """Reset API manager circuit breakers and performance metrics"""
        self.api_manager.reset_circuit_breakers()
        logger.info("API Manager reset completed")

    def get_retry_manager_status(self) -> Dict:
        """Get retry manager and circuit breaker status"""
        return {
            'circuit_breaker_status': self.retry_manager.get_circuit_breaker_status(),
            'max_retries': self.retry_manager.max_retries,
            'base_delay': self.retry_manager.base_delay,
            'strategy': self.retry_manager.strategy.value
        }

    def reset_retry_manager(self):
        """Reset retry manager circuit breakers"""
        self.retry_manager.reset_all_circuit_breakers()
        logger.info("Retry Manager reset completed")

    def detect_gaps(self, symbol: str = None, days_back: int = 30) -> List[Dict]:
        """Detect gaps in data for specified symbol or all symbols"""
        return self.gap_detector.detect_gaps(symbol, days_back)

    def analyze_data_completeness(self, symbol: str = None, days_back: int = 7) -> Dict:
        """Analyze overall data completeness and quality metrics"""
        return self.gap_detector.analyze_data_completeness(symbol, days_back)

    def get_gap_report(self, symbol: str = None, days_back: int = 30) -> Dict:
        """Generate comprehensive gap report"""
        return self.gap_detector.get_gap_report(symbol, days_back)

    async def backfill_gaps(self, gaps: List[Dict] = None, priority: str = 'auto') -> Dict:
        """Backfill data gaps with automatic prioritization"""
        if gaps is None:
            # Detect gaps first
            gaps = self.gap_detector.detect_gaps(days_back=30)

        return await self.backfill_manager.backfill_gaps(gaps, priority)

    async def schedule_backfill(self, symbol: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Schedule comprehensive backfill for missing data"""
        return await self.backfill_manager.schedule_backfill(symbol, start_date, end_date)

    def get_backfill_status(self) -> Dict:
        """Get backfill manager status and configuration"""
        return {
            'max_concurrent_backfills': self.backfill_manager.max_concurrent_backfills,
            'chunk_size_hours': self.backfill_manager.chunk_size_hours,
            'rate_limit_delay': self.backfill_manager.rate_limit_delay,
            'max_backfill_age_days': self.backfill_manager.max_backfill_age_days
        }

    async def fetch_multiple_symbols_concurrent(self, symbols: List[str] = None, **params) -> Dict:
        """Fetch data for multiple symbols concurrently"""
        if symbols is None:
            symbols = self._get_supported_symbols()

        return await self.concurrent_fetcher.fetch_multiple_symbols(symbols, **params)

    async def process_backfill_batch_concurrent(self, backfill_tasks: List[Dict], max_concurrent: int = 3) -> Dict:
        """Process multiple backfill tasks concurrently"""
        return await self.concurrent_fetcher.process_backfill_batch(backfill_tasks, max_concurrent)

    def get_parallel_processing_stats(self) -> Dict:
        """Get parallel processing statistics"""
        return self.parallel_processor.get_processing_stats()

    def clear_parallel_processing_cache(self, older_than_hours: int = 24):
        """Clear old completed tasks from parallel processor"""
        self.parallel_processor.clear_completed_tasks(older_than_hours)
        logger.info(f"Cleared parallel processing cache (tasks older than {older_than_hours}h)")

    async def stop(self):
        """Stop the data fetching bot"""
        logger.info("🛑 Stopping Data Fetching Bot...")
        self.running = False

        # Stop parallel processor
        await self.parallel_processor.stop()

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