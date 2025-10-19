# 🚀 Real-Time Liquidation Data Monitor

A powerful real-time liquidation data monitoring system for cryptocurrency futures markets. Capture live liquidation events as they happen and use them for market analysis, trading signals, and research.

## ⚡ Quick Start

### Install Dependencies
```bash
pip install -r requirements_liquidation.txt
```

### Run the Monitor

#### Option 1: Use the Easy Launcher (Recommended)
```bash
# Monitor all major cryptocurrencies
./run_liquidation_monitor.sh --all

# Monitor specific cryptocurrencies
./run_liquidation_monitor.sh --symbols "BTC ETH BNB"

# Monitor for 24 hours only
./run_liquidation_monitor.sh --symbols BTC --duration 24
```

#### Option 2: Direct Python Execution
```bash
# Monitor BTC and ETH
python3 realtime_liquidation_monitor.py --symbols BTC ETH

# Monitor all available symbols
python3 realtime_liquidation_monitor.py --all

# Monitor with custom output directory
python3 realtime_liquidation_monitor.py --symbols BTC ETH --output /path/to/custom/dir
```

## 📊 What It Does

### Real-Time Data Capture
- ✅ **Live WebSocket Connections**: Connects directly to Binance Futures WebSocket streams
- ✅ **Instant Liquidation Detection**: Captures liquidation events as they occur
- ✅ **Multi-Symbol Monitoring**: Track multiple cryptocurrencies simultaneously
- ✅ **Automatic Reconnection**: Handles connection drops gracefully

### Comprehensive Data Collection
- **Liquidation Details**: Price, quantity, side (LONG/SHORT), timestamp
- **Volume Tracking**: Total liquidation amounts and frequencies
- **Symbol-Specific Data**: Separate files for each cryptocurrency
- **Real-Time Statistics**: Live monitoring dashboard

### Smart Data Management
- **Automatic Saving**: Data saves every 100 liquidations
- **Compressed Archives**: Hourly compressed backups
- **Memory Efficient**: Prevents memory buildup during long runs
- **Structured Storage**: CSV format with timestamps

## 🎯 Available Cryptocurrencies

### Major Assets (Recommended)
```
BTC   - Bitcoin
ETH   - Ethereum
BNB   - Binance Coin
ADA   - Cardano
XRP   - Ripple
SOL   - Solana
DOT   - Polkadot
DOGE  - Dogecoin
AVAX  - Avalanche
LTC   - Litecoin
```

### Additional Assets
```
LINK  - Chainlink
UNI   - Uniswap
ALGO  - Algorand
VET   - VeChain
ICP   - Internet Computer
FIL   - Filecoin
TRX   - Tron
ETC   - Ethereum Classic
XLM   - Stellar
THETA - Theta Network
```

## 📁 Output Structure

```
data/realtime_liquidations/
├── btc/
│   ├── btc_liquidations_realtime.csv          # Live data (updates continuously)
│   └── btc_liquidations_20251018_14.csv.gz    # Hourly compressed archives
├── eth/
│   ├── eth_liquidations_realtime.csv
│   └── eth_liquidations_20251018_14.csv.gz
└── [other_symbols]/
```

### CSV Data Format
```csv
timestamp,symbol,side,price,quantity,amount,event_time,event_type
2025-10-18 14:30:15.123,BTC,LONG,45000.50,0.5,22500.25,1697583015123,liquidation
2025-10-18 14:30:16.456,BTC,SHORT,44980.00,1.2,53976.00,1697583016456,liquidation
```

## 🎮 Usage Examples

### Basic Monitoring
```bash
# Monitor Bitcoin liquidations only
./run_liquidation_monitor.sh --symbols BTC

# Monitor top 3 cryptocurrencies
./run_liquidation_monitor.sh --symbols "BTC ETH BNB"
```

### Time-Limited Sessions
```bash
# Monitor for exactly 24 hours
./run_liquidation_monitor.sh --symbols BTC --duration 24

# Quick 1-hour test run
./run_liquidation_monitor.sh --symbols "BTC ETH" --duration 1
```

### Custom Output Location
```bash
# Save to specific directory
./run_liquidation_monitor.sh --all --output /home/user/liquidation_data

# Use relative path
./run_liquidation_monitor.sh --symbols BTC --output ./my_liquidations
```

### Advanced Multi-Symbol Monitoring
```bash
# Monitor major altcoins
./run_liquidation_monitor.sh --symbols "ETH BNB ADA XRP SOL"

# Monitor DeFi tokens
./run_liquidation_monitor.sh --symbols "UNI LINK AAVE SUSHI"
```

## 📊 Live Statistics Dashboard

While running, the monitor displays real-time statistics:

```
======================================================================
📊 REAL-TIME LIQUIDATION MONITOR
======================================================================
⏰ Running since: 2025-10-18 14:30:00
🔗 Connection: ✅ Connected
💰 Total Liquidations: 1,247
⏳ Time remaining: 23:25:30

📈 Per-Symbol Statistics:
----------------------------------------
  BTC: Count: 456 | Volume: $12,345,678 | Recent (30min): 23
  ETH: Count: 312 | Volume: $8,901,234  | Recent (30min): 15
  BNB: Count: 234 | Volume: $3,456,789  | Recent (30min): 8

📁 Data saved to: data/realtime_liquidations
💡 Press Ctrl+C to stop monitoring
======================================================================
```

## 🎯 Liquidation Data Applications

### Trading Strategies
- **Capitulation Detection**: Large liquidation clusters signal potential reversals
- **Mean Reversion**: Trade against liquidation-induced price moves
- **Scalping Opportunities**: Quick profits from liquidation cascades
- **Risk Management**: Avoid positions during high liquidation periods

### Market Analysis
- **Fear & Greed Indicator**: Liquidation spikes correlate with market sentiment
- **Support/Resistance**: Liquidation walls create price magnets
- **Market Stress**: Monitor liquidation volumes as volatility indicators
- **Institutional Activity**: Track large player liquidation patterns

### Research Applications
- **Market Microstructure**: How liquidations affect price formation
- **Volatility Studies**: Liquidation spikes and volatility correlation
- **Behavioral Finance**: Trader capitulation patterns
- **Risk Assessment**: Systemic risk from correlated liquidations

## 🔧 Configuration & Customization

### Binance API Keys
The monitor uses your existing Binance API keys from `config.env`:
```bash
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key
```

### Custom Symbol Lists
Add new symbols by modifying the `valid_symbols` list in the Python script.

### Data Retention
- **Live Data**: Continuous updates to CSV files
- **Archives**: Hourly compressed backups
- **Memory**: Keeps last 1000 records per symbol

## 🚨 Important Notes

### Rate Limits
- Respects Binance WebSocket rate limits
- Automatic reconnection on connection drops
- Graceful handling of API restrictions

### Data Accuracy
- **Real-time**: Data captured at event time
- **Complete**: All liquidation events are captured
- **Accurate**: Direct from exchange WebSocket streams

### System Requirements
- **Python 3.8+**: Required for async WebSocket support
- **Stable Internet**: Continuous connection for real-time data
- **Disk Space**: ~10MB/hour per actively traded symbol

### Legal & Compliance
- **Exchange Terms**: Follows Binance API terms of service
- **Data Usage**: For personal/research use only
- **No Market Manipulation**: Do not use for manipulative trading

## 🛠️ Troubleshooting

### Connection Issues
```bash
# Check internet connection
ping 8.8.8.8

# Verify API keys in config.env
cat config.env

# Check Binance Futures API status
curl https://api.binance.com/api/v3/ping
```

### Permission Issues
```bash
# Make script executable
chmod +x run_liquidation_monitor.sh

# Check write permissions
mkdir -p data/realtime_liquidations
ls -la data/
```

### Data Not Saving
```bash
# Check disk space
df -h

# Verify output directory
ls -la data/realtime_liquidations/

# Check file permissions
touch data/realtime_liquidations/test.txt
```

## 📈 Performance Metrics

### Expected Data Volume
- **BTC**: 100-500 liquidations/day (high activity)
- **ETH**: 50-200 liquidations/day (moderate activity)
- **Altcoins**: 10-50 liquidations/day (varies by market cap)

### System Performance
- **Memory Usage**: ~50MB for 10 symbols
- **CPU Usage**: <5% on modern hardware
- **Network**: ~10KB/minute per symbol

## 🎯 Next Steps

1. **Start Monitoring**: Run `./run_liquidation_monitor.sh --all`
2. **Analyze Patterns**: Study liquidation clusters and market correlations
3. **Build Strategies**: Develop algorithms based on liquidation signals
4. **Scale Up**: Add more symbols and implement alerting

---

**Happy Monitoring! 🚀📊**

*Real-time liquidation data is one of the most powerful signals in cryptocurrency markets. Use it wisely for better trading decisions.*




