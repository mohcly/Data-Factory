#!/usr/bin/env python3
"""
Basic test script for Data Fetching Bot
Tests database initialization and basic API connectivity
"""

import asyncio
import logging
import sys
from pathlib import Path
import pandas as pd

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from database.database_manager import DatabaseManager
from api.binance_client import BinanceAPIClient
from api.coingecko_client import CoinGeckoAPIClient
from data_processing.data_validator import DataValidator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_database():
    """Test database initialization and basic operations"""
    logger.info("🗄️ Testing Database Manager...")

    try:
        db_manager = DatabaseManager()
        db_info = db_manager.get_database_info()

        logger.info("✅ Database initialized successfully")
        logger.info(f"   Database path: {db_info['database_path']}")
        logger.info(f"   Is new database: {db_info['is_new_database']}")
        logger.info(f"   Total records: {db_info['total_records']}")

        # Test configuration
        supported_symbols = db_manager.get_config_value('supported_symbols')
        logger.info(f"   Supported symbols: {supported_symbols}")

        return True

    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        return False


async def test_api_clients():
    """Test API client initialization and connectivity"""
    logger.info("🌐 Testing API Clients...")

    success_count = 0
    total_clients = 0

    # Test Binance client
    total_clients += 1
    try:
        binance_client = BinanceAPIClient()
        logger.info("✅ Binance client initialized")

        # Test connectivity
        healthy = await binance_client.ping()
        if healthy:
            logger.info("✅ Binance API connectivity test passed")
            success_count += 1
        else:
            logger.warning("⚠️ Binance API connectivity test failed")

    except Exception as e:
        logger.error(f"❌ Binance client test failed: {e}")

    # Test CoinGecko client
    total_clients += 1
    try:
        coingecko_client = CoinGeckoAPIClient()
        logger.info("✅ CoinGecko client initialized")

        # Test connectivity
        healthy = await coingecko_client.ping()
        if healthy:
            logger.info("✅ CoinGecko API connectivity test passed")
            success_count += 1
        else:
            logger.warning("⚠️ CoinGecko API connectivity test failed")

    except Exception as e:
        logger.error(f"❌ CoinGecko client test failed: {e}")

    logger.info(f"API connectivity test results: {success_count}/{total_clients} clients successful")
    return success_count > 0


async def test_data_validator():
    """Test data validation functionality"""
    logger.info("✅ Testing Data Validator...")

    try:
        validator = DataValidator()

        # Test data
        test_data = [
            {
                'timestamp': pd.to_datetime('2025-01-01T00:00:00'),
                'open': 50000.0,
                'high': 51000.0,
                'low': 49500.0,
                'close': 50500.0,
                'volume': 1000.0
            },
            {
                'timestamp': pd.to_datetime('2025-01-01T01:00:00'),
                'open': 50500.0,
                'high': 51500.0,
                'low': 50000.0,
                'close': 51200.0,
                'volume': 1200.0
            }
        ]

        # Validate data
        result = validator.validate_ohlcv_data(test_data, 'test_api')

        logger.info("✅ Data validation completed")
        logger.info(f"   Is valid: {result['is_valid']}")
        logger.info(f"   Quality score: {result['quality_score']}")
        logger.info(f"   Data points: {result['data_points']}")
        logger.info(f"   Errors: {len(result['errors'])}")
        logger.info(f"   Warnings: {len(result['warnings'])}")

        # Test normalization
        normalized_data = validator.normalize_data(test_data, 'BTCUSDT')
        logger.info(f"✅ Data normalization completed ({len(normalized_data)} records)")

        return result['is_valid']

    except Exception as e:
        logger.error(f"❌ Data validator test failed: {e}")
        return False


async def main():
    """Run all tests"""
    logger.info("🚀 Starting Data Fetching Bot Tests...")
    logger.info("=" * 50)

    tests = [
        ("Database Manager", test_database),
        ("API Clients", test_api_clients),
        ("Data Validator", test_data_validator)
    ]

    results = []

    for test_name, test_func in tests:
        logger.info(f"\n📋 Running {test_name} test...")
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"❌ {test_name} test failed with exception: {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("📊 TEST SUMMARY")

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"   {test_name}: {status}")
        if result:
            passed += 1

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All tests passed! The bot foundation is working correctly.")
        return 0
    else:
        logger.warning("⚠️ Some tests failed. Please check the implementation.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)