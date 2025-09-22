# 🏭 Crypto Data-Factory

## Overview

The **Crypto Data-Factory** is a comprehensive, production-grade cryptocurrency data acquisition and processing system designed to ensure reliable, continuous market data collection with advanced error handling, recovery mechanisms, and multi-API redundancy.

## 🎯 Mission Statement

To provide reliable, high-quality cryptocurrency market data with zero tolerance for data gaps while maintaining system resilience against API failures, network issues, and data quality problems.

## 🏗️ Project Organization

This repository follows a clean, organized structure that separates concerns and makes development and maintenance efficient:

### **Source Code Organization**
- **`src/core/`** - Main application logic and core functionality
- **`src/data/`** - Data processing and database operations
- **`src/utils/`** - Utility functions and helpers
- **`src/tests/`** - Test files and testing utilities

### **Configuration Management**
- **`config/`** - All configuration files and dependencies
- **`scripts/`** - Executable scripts and automation tools

### **Data Management**
- **`data/`** - Input data files and datasets
- **`databases/`** - Generated database files (auto-created)
- **`logs/`** - Application logs and runtime information
- **`pre-processing/`** - Data preprocessing operations

### **Documentation Structure**
- **`memory-bank/`** - Comprehensive documentation and knowledge base
- **`docs/`** - User documentation and project notes
- **`assets/`** - Static assets and configuration files

## ✨ Key Features

### 🗄️ **Fresh Database Creation**
- **Brand new database** created on every startup
- **Automatic schema initialization** with all required tables and indexes
- **Comprehensive configuration** setup with default values
- **Database validation** and recovery mechanisms

### 🔄 **Multi-API Data Fetching**
- **Primary APIs**: Binance, CoinGecko, Yahoo Finance
- **Backup APIs**: CoinMarketCap, Kraken, Coinbase (planned)
- **Automatic failover** when primary APIs fail
- **Intelligent load balancing** across available APIs

### 🛡️ **Advanced Error Handling**
- **Exponential backoff** retry mechanisms
- **Circuit breaker patterns** for failing APIs
- **Network resilience** with connection pooling
- **Graceful degradation** during outages

### 📊 **Data Quality Assurance**
- **OHLCV validation** and integrity checks
- **Duplicate detection** and removal
- **Cross-API data validation** (when possible)
- **Quality scoring** for each data point

### 📈 **Real-time Monitoring**
- **Performance metrics** collection
- **Health checks** for all APIs
- **Error logging** and alerting
- **Database status** monitoring

## 📁 Repository Structure

```
Crypto/Data-factory/
├── 📄 README.md                      # This file
├── 📁 src/                           # Source code
│   ├── 📁 core/                      # Core application files
│   │   └── 🐍 crypto_data_fetcher_bot.py  # Main application
│   ├── 📁 data/                      # Data processing files
│   │   ├── create_correlated_groups_tables.py
│   │   ├── update_correlated_groups_db.py
│   │   ├── fetch_missing_crypto_data.py
│   │   └── data_preprocessor.py
│   ├── 📁 utils/                     # Utility functions
│   └── 📁 tests/                     # Test files
├── 📁 config/                        # Configuration files
│   ├── 📋 requirements.txt           # Python dependencies
│   └── 📋 environment.yml            # Environment configuration
├── 🛠️ scripts/                       # Scripts and tools
│   └── 🐚 start_bot.sh               # Startup script
├── 📊 memory-bank/                   # Documentation and planning
│   ├── 📁 consolidated/              # Consolidated indexes and summaries
│   ├── 📁 technical/                 # Technical documentation
│   ├── 📁 enterprise/                # Enterprise and security
│   ├── 📁 core/                      # Core operations hub
│   ├── 📁 planning/                  # Planning and strategy
│   ├── 📁 multi-asset/               # Multi-asset documentation
│   ├── 📁 reports/                   # Reports and metrics
│   └── 📁 docs/                      # Comprehensive documentation
├── 🗄️ databases/                     # Database files (auto-created)
├── 📝 logs/                          # Application logs (auto-created)
├── 📊 data/                          # Data directories
│   ├── 📁 raw/                       # Raw data files
│   ├── 📁 processed/                 # Processed data files
│   └── 📁 missing_data/              # Missing data handling
├── 🔧 pre-processing/                # Data preprocessing scripts
├── 📚 docs/                          # Documentation
│   ├── 📁 Notes/                     # Project notes
│   └── 📁 api-docs/                  # API documentation
└── 📦 assets/                        # Static assets and configs
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Internet connection
- API keys (optional, for enhanced rate limits)

### Installation

1. **Clone and navigate to the repository:**
   ```bash
   cd /Users/mohamedcoulibaly/MVP/Crypto/Data-factory
   ```

2. **Install dependencies:**
   ```bash
   pip install -r config/requirements.txt
   ```

3. **Start the bot:**
   ```bash
   ./scripts/start_bot.sh
   ```

   Or run directly:
   ```bash
   python src/core/crypto_data_fetcher_bot.py
   ```

### What Happens on Startup

1. **Database Creation**: A brand new database is created with timestamp
2. **Schema Setup**: All tables, indexes, and views are automatically created
3. **Configuration**: Default settings are initialized
4. **Data Collection**: Bot begins fetching data from multiple APIs
5. **Continuous Operation**: Runs 24/7 with 1-hour collection cycles

## 📊 Supported Cryptocurrencies

| Symbol | Name | Primary API | Backup API |
|--------|------|-------------|------------|
| BTCUSDT | Bitcoin | Binance | CoinGecko |
| ETHUSDT | Ethereum | Binance | CoinGecko |
| SOLUSDT | Solana | Binance | CoinGecko |
| ADAUSDT | Cardano | Binance | CoinGecko |

## 🔧 Configuration

### Environment Variables
```bash
# Optional: Binance API credentials for enhanced limits
BINANCE_API_KEY=your_api_key_here
BINANCE_SECRET_KEY=your_secret_key_here

# Optional: CoinMarketCap API key
COINMARKETCAP_API_KEY=your_api_key_here
```

### Database Configuration
- **Location**: `databases/crypto_data_YYYYMMDD_HHMMSS.db`
- **Schema**: Auto-created with optimized indexes
- **Backup**: Automatic timestamp-based naming
- **Retention**: Configurable (default: 10 years)

## 📈 Monitoring & Logs

### Log Files
- **Main Log**: `logs/bot.log` - Detailed application logs
- **Error Log**: `logs/error.log` - Error tracking and debugging
- **Performance Log**: `logs/performance.log` - API performance metrics

### Database Views
- **all_crypto_data**: All validated cryptocurrency data
- **daily_stats**: Daily aggregated statistics
- **system_config**: System configuration and metadata

## 🛠️ Development

### Adding New APIs
1. Create new API client class inheriting from `BaseAPIClient`
2. Implement required methods (`make_request`, `is_healthy`)
3. Add configuration to `api_configs` dictionary
4. Update supported symbols mapping

### Database Schema Changes
1. Update `DatabaseManager._create_tables()` method
2. Update corresponding indexes in `_create_indexes()`
3. Update views in `_create_views()` if needed
4. Test with fresh database creation

### Testing
```bash
# Run unit tests
python -m pytest tests/

# Run with coverage
python -m pytest --cov=src tests/

# Run integration tests
python -m pytest tests/integration/
```

## 📊 Performance Metrics

### Target Performance
- **Uptime**: 99.9% system availability
- **Data Accuracy**: 99.99% data point accuracy
- **Gap Recovery**: < 5 minutes for API failures
- **Response Time**: < 2 seconds average API response

### Current Status
- **Database Creation**: ✅ Automated and reliable
- **API Integration**: 🔄 In development
- **Error Recovery**: ✅ Basic implementation
- **Monitoring**: ✅ Logging and basic metrics

## 🚨 Troubleshooting

### Common Issues

#### 1. **Database Creation Fails**
```bash
# Check permissions
ls -la databases/

# Create directory manually
mkdir -p databases
chmod 755 databases

# Check disk space
df -h
```

#### 2. **API Rate Limiting**
- The bot respects all API rate limits
- Automatic throttling implemented
- Check logs for rate limit warnings

#### 3. **Network Connectivity Issues**
- Bot includes retry mechanisms
- Automatic failover to backup APIs
- Graceful handling of network outages

#### 4. **Missing Dependencies**
```bash
# Install all requirements
pip install -r requirements.txt

# Install specific package
pip install pandas requests
```

### Debug Mode
Run with debug logging:
```bash
export LOG_LEVEL=DEBUG
python crypto_data_fetcher_bot.py
```

## 📈 Roadmap

### ✅ Completed (Phase 1)
- [x] Memory-bank structure setup
- [x] DatabaseManager implementation
- [x] Fresh database creation system
- [x] Basic logging and monitoring
- [x] Main bot architecture

### 🔄 In Progress (Phase 1)
- [ ] Multi-API data fetching
- [ ] Data validation pipeline
- [ ] Error recovery mechanisms
- [ ] Performance optimization

### 📋 Planned (Phase 2-4)
- [ ] Advanced API failover logic
- [ ] Gap detection and recovery
- [ ] Real-time monitoring dashboard
- [ ] Production deployment tools

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature-name`
3. **Make your changes** following the existing code style
4. **Add tests** for new functionality
5. **Update documentation** in the memory-bank
6. **Submit a pull request**

### Code Style
- Use descriptive variable and function names
- Add docstrings to all public methods
- Follow PEP 8 style guidelines
- Include type hints where appropriate

## 📄 License

This project is part of the MVP Crypto Trading System. See main repository for license information.

## 🔗 Related Projects

- **[Timeseries-forcasting](../../Timeseries-forcasting/)**: Main trading system that uses this data
- **[MVP](../../)**: Main repository containing all crypto trading components

## 📞 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the logs in `logs/bot.log`
3. Check existing issues in the repository
4. Create a new issue with detailed information

---

## 🎯 Success Metrics

### Database Creation
- ✅ **Fresh Start**: Brand new database created on startup
- ✅ **Schema Complete**: All tables, indexes, and views created
- ✅ **Configuration Set**: Default settings initialized
- ✅ **Validation Ready**: Database integrity checks in place

### Data Collection
- 🔄 **API Integration**: Multi-API support with failover
- 🔄 **Error Handling**: Comprehensive recovery mechanisms
- 🔄 **Quality Assurance**: Data validation and integrity
- 🔄 **Performance**: Optimized for continuous operation

### System Reliability
- ✅ **Monitoring**: Comprehensive logging and metrics
- ✅ **Recovery**: Automatic error recovery and retry
- ✅ **Maintenance**: Self-healing capabilities
- 🔄 **Scalability**: Designed for expansion

---

**Data-Factory Status**: 🔄 **ACTIVE DEVELOPMENT** | Version: 1.0.0 | Last Updated: 2025-09-21

*This system ensures reliable cryptocurrency data acquisition for quantitative trading applications.*
