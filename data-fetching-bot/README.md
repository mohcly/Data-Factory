# Data Fetching Bot

A comprehensive, production-grade cryptocurrency data acquisition system designed to ensure reliable, continuous market data collection with advanced error handling, recovery mechanisms, and multi-API redundancy.

## 🎯 Overview

The Data Fetching Bot eliminates data gaps and provides a robust foundation for quantitative trading and analysis applications by:

- **Multi-API Integration**: Support for Binance, CoinGecko, Yahoo Finance, and other data sources
- **Intelligent Failover**: Automatic switching between APIs when failures occur
- **Data Validation**: Comprehensive OHLCV data validation and quality scoring
- **Error Recovery**: Automated gap detection and backfill processing
- **Performance Monitoring**: Real-time tracking of API health and performance metrics

## 📋 Features

### Core Functionality
- ✅ **24/7 Data Collection** - Continuous fetching of cryptocurrency data
- ✅ **Multi-API Support** - Binance and CoinGecko integration with more planned
- ✅ **Fresh Database Creation** - Automatic schema initialization on startup
- ✅ **Data Validation** - OHLCV integrity checks and quality scoring
- ✅ **Error Handling** - Comprehensive error tracking and recovery
- ✅ **Rate Limiting** - Intelligent request throttling to respect API limits

### Database Management
- ✅ **SQLite Backend** - Lightweight, file-based database
- ✅ **Schema Auto-Creation** - Tables, indexes, and views created automatically
- ✅ **Data Deduplication** - Prevents duplicate entries across sources
- ✅ **Performance Tracking** - API performance metrics storage
- ✅ **Error Logging** - System error tracking and resolution

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Required packages (see `requirements.txt`)

### Installation

1. **Clone and navigate to the project**:
   ```bash
   cd /workspace/data-fetching-bot
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run basic tests**:
   ```bash
   python test_basic.py
   ```

4. **Start the bot**:
   ```bash
   python -m src.data_fetching_bot
   ```

### Configuration

The bot uses environment variables for API credentials:

```bash
# Binance API (optional - works without for public endpoints)
export BINANCE_API_KEY="your_api_key"
export BINANCE_API_SECRET="your_api_secret"

# Enable testnet mode (optional)
export BINANCE_TESTNET="true"
```

## 🏗️ Architecture

### Project Structure
```
data-fetching-bot/
├── src/
│   ├── __init__.py
│   ├── data_fetching_bot.py     # Main application
│   ├── database/
│   │   ├── __init__.py
│   │   └── database_manager.py  # Database management
│   ├── api/
│   │   ├── __init__.py
│   │   ├── base_api_client.py   # Base API client class
│   │   ├── binance_client.py    # Binance API integration
│   │   └── coingecko_client.py  # CoinGecko API integration
│   └── data_processing/
│       ├── __init__.py
│       └── data_validator.py    # Data validation and processing
├── data/                        # Database and data files
├── logs/                        # Application logs
├── config/                      # Configuration files
├── tests/                       # Test files
├── requirements.txt             # Python dependencies
├── test_basic.py               # Basic functionality tests
└── README.md                   # This file
```

### Relationship with Memory-Bank

The data-fetching-bot is designed to work alongside the memory-bank knowledge repository:

- **Documentation**: References `/workspace/memory-bank/docs/` for technical documentation
- **Knowledge**: Uses `/workspace/memory-bank/consolidated/` for implementation guidance
- **Planning**: Follows `/workspace/memory-bank/planning/tasks/` for development tasks

### Database Schema

#### Core Tables
- **`crypto_data`** - Main cryptocurrency data storage
- **`system_config`** - Bot configuration and metadata
- **`api_performance`** - API performance tracking
- **`data_gaps`** - Data gap tracking and recovery
- **`system_errors`** - System error logging

#### Views
- **`all_crypto_data`** - Validated data across all assets
- **`daily_stats`** - Daily aggregated statistics

## 🔧 Usage

### Basic Usage

```python
from src.data_fetching_bot import DataFetchingBot

async def main():
    bot = DataFetchingBot()
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())
```

### Fetch Historical Data

```python
# Fetch historical data for backfilling
inserted_count = await bot.fetch_historical_data(
    symbol="BTCUSDT",
    start_date="2024-01-01",
    end_date="2024-12-31"
)
```

### Get System Information

```python
# Get database information
db_info = bot.get_database_info()

# Get API performance metrics
api_performance = bot.get_api_performance()
```

## 📊 Monitoring

### Logs
- **Console Output** - Real-time status and errors
- **File Logs** - Detailed logs in `logs/bot.log`

### Database Metrics
- **Database Path** - Location of SQLite database
- **Record Counts** - Number of records per table
- **Data Coverage** - Time ranges for each symbol

### API Health
- **Request Success Rate** - Percentage of successful requests
- **Average Response Time** - API response performance
- **Health Status** - Current API health status

## 🧪 Testing

### Run All Tests
```bash
python test_basic.py
```

### Test Individual Components
```python
# Test database functionality
from src.database.database_manager import DatabaseManager
db = DatabaseManager()
db_info = db.get_database_info()

# Test API clients
from src.api.binance_client import BinanceAPIClient
client = BinanceAPIClient()
await client.ping()

# Test data validation
from src.data_processing.data_validator import DataValidator
validator = DataValidator()
result = validator.validate_ohlcv_data(test_data)
```

## 📈 Performance Requirements

### Target Metrics
- **Data Fetching Rate**: 1000+ requests per minute
- **API Success Rate**: >99.9% successful requests
- **System Uptime**: 99.9% availability
- **Response Time**: <2 seconds average API response
- **Data Accuracy**: 99.99% data point accuracy

### Rate Limiting
- **Binance**: 1200 requests/minute (authenticated)
- **CoinGecko**: 30 requests/minute (free tier)

## 🔒 Security

### API Key Management
- Environment variable storage
- Secure credential handling
- API key rotation support

### Data Security
- HTTPS-only API communication
- Secure database connections
- Audit trails for data modifications

## 🚨 Error Handling

### Automatic Recovery
- **API Failover** - Switch to backup APIs on failure
- **Gap Detection** - Identify missing data periods
- **Backfill Processing** - Historical data recovery
- **Retry Logic** - Exponential backoff for failed requests

### Error Categories
- **API Connection Errors** - Network and authentication issues
- **Data Validation Errors** - Invalid or corrupted data
- **Rate Limiting** - API quota exceeded
- **System Errors** - Database and processing failures

## 🔮 Future Enhancements

### Planned Features
- **Real-time WebSocket Integration** - Live data streaming
- **Additional API Support** - Yahoo Finance, CoinMarketCap
- **Machine Learning** - Predictive gap filling
- **Data Visualization** - Built-in monitoring dashboard
- **Cloud Deployment** - AWS/GCP/Azure support

### Scalability Improvements
- **Distributed Processing** - Multi-node data fetching
- **Container Orchestration** - Kubernetes deployment
- **Auto-scaling** - Dynamic resource allocation

## 📝 License

This project is a standalone data fetching bot for cryptocurrency data acquisition, designed to work alongside the memory-bank knowledge repository for quantitative trading applications.

## 🆘 Support

For issues and questions:
1. Check the logs in `logs/bot.log`
2. Review the test output from `test_basic.py`
3. Verify API connectivity with individual clients
4. Check database integrity with `DatabaseManager.get_database_info()`

---

**Document Status**: ✅ **IMPLEMENTED** | Version: 1.0.0 | Last Updated: 2025-01-22