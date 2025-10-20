#!/bin/bash

# Real-Time Orderbook Monitor Launcher
# Easy-to-use script for running the orderbook data monitor

echo -e "\033[92;1m🕯️ 5-Minute Orderbook Candle Monitor with Buffer Recovery\033[0m"
echo -e "\033[95;1m=======================================================\033[0m"
echo ""

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Creates 5-minute candles from real-time orderbook data with buffer recovery."
    echo "Automatically recovers available historical data before starting real-time monitoring."
    echo "Each candle contains OHLC, spread analysis, and volume metrics."
    echo ""
    echo "Options:"
    echo "  --all                 Monitor all major cryptocurrencies"
    echo "  --quality-data        Monitor symbols from quality data categories"
    echo "  --symbols SYMBOLS     Monitor specific symbols (space separated)"
    echo "  --duration HOURS      Run for specific duration in hours"
    echo "  --output DIR          Custom output directory (default: data/orderbook_5min_candles)"
    echo "  --verbose             Show verbose logging with all WebSocket messages"
    echo "  --depth LEVELS        Orderbook depth to capture (default: 10)"
    echo "  --storage-efficient   Enable storage-efficient mode (reduced data volume)"
    echo "  --no-buffer-recovery  Skip buffer data recovery on startup"
    echo "  --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --quality-data                           # Monitor quality data symbols"
    echo "  $0 --quality-data --storage-efficient       # Monitor with reduced data volume"
    echo "  $0 --all                                    # Monitor all major cryptos"
    echo "  $0 --symbols \"BTC ETH BNB\"                  # Monitor BTC, ETH, BNB"
    echo "  $0 --quality-data --verbose                 # Monitor with verbose WebSocket logs"
    echo "  $0 --symbols BTC --depth 5                  # Monitor BTC with minimal depth"
    echo "  $0 --quality-data --duration 24             # Monitor for 24 hours"
    echo "  $0 --all --output /custom/path              # Custom output directory"
    echo ""
    echo "Major cryptocurrencies available:"
    echo "  BTC, ETH, BNB, ADA, XRP, SOL, DOT, DOGE, AVAX, LTC"
    echo "  LINK, UNI, ALGO, VET, ICP, FIL, TRX, ETC, XLM, THETA"
    echo ""
echo "Quality data symbols (--quality-data):"
echo "  AAVE, ADA, AVAX, BCH, BNB, BTC, DOGE, DOT, ETC, ETH"
echo "  FIL, HBAR, LINK, LTC, SOL, THETA, TRX, UNI"
echo "  USDC, XLM, XMR, XRP, ZEC (23 total - Binance Futures only)"
    echo ""
}

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 is not installed or not in PATH"
    echo "Please install Python3 and try again"
    exit 1
fi

# Check if required packages are installed
python3 -c "import websockets, asyncio, pandas" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "⚠️  Installing required packages..."
    pip install websockets pandas
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install required packages. Please run:"
        echo "   pip install websockets pandas"
        exit 1
    fi
fi

# Parse command line arguments
if [ $# -eq 0 ]; then
    echo "❌ No arguments provided."
    echo ""
    show_usage
    exit 1
fi

# Build Python command
PYTHON_CMD="python3 realtime_orderbook_monitor.py"

# Process arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_usage
            exit 0
            ;;
        --all)
            PYTHON_CMD="$PYTHON_CMD --all"
            shift
            ;;
        --quality-data)
            PYTHON_CMD="$PYTHON_CMD --quality-data"
            shift
            ;;
        --symbols)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "❌ Error: --symbols requires a value"
                echo "Example: --symbols \"BTC ETH BNB\""
                exit 1
            fi
            PYTHON_CMD="$PYTHON_CMD --symbols $2"
            shift 2
            ;;
        --duration)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "❌ Error: --duration requires a number"
                echo "Example: --duration 24"
                exit 1
            fi
            PYTHON_CMD="$PYTHON_CMD --duration $2"
            shift 2
            ;;
        --output)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "❌ Error: --output requires a directory path"
                exit 1
            fi
            PYTHON_CMD="$PYTHON_CMD --output $2"
            shift 2
            ;;
        --verbose)
            PYTHON_CMD="$PYTHON_CMD --verbose"
            shift
            ;;
        --depth)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "❌ Error: --depth requires a number"
                echo "Example: --depth 10"
                exit 1
            fi
            PYTHON_CMD="$PYTHON_CMD --depth $2"
            shift 2
            ;;
        --storage-efficient)
            PYTHON_CMD="$PYTHON_CMD --storage-efficient"
            shift
            ;;
        --no-buffer-recovery)
            PYTHON_CMD="$PYTHON_CMD --no-buffer-recovery"
            shift
            ;;
        *)
            echo "❌ Unknown option: $1"
            echo ""
            show_usage
            exit 1
            ;;
    esac
done

echo "🔧 Executing: $PYTHON_CMD"
echo ""
echo -e "\033[94m💡 Controls:\033[0m"
echo -e "   \033[96m• Press Ctrl+C to stop monitoring\033[0m"
echo -e "   \033[96m• Statistics update every 30 seconds\033[0m"
echo -e "   \033[96m• Recovers buffer data, then creates 5-minute candles every 5 minutes\033[0m"
echo ""
echo -e "\033[92;1m📊 Starting orderbook monitoring...\033[0m"
echo -e "\033[95;1m=====================================\033[0m"

# Execute the Python command
exec $PYTHON_CMD
