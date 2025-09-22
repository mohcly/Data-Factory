#!/bin/bash

# Crypto Data Fetcher Bot - Startup Script
# This script initializes and starts the data fetching bot

echo "🚀 Starting Crypto Data Fetcher Bot..."
echo "====================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is not installed or not in PATH"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "src/core/crypto_data_fetcher_bot.py" ]; then
    echo "❌ Error: crypto_data_fetcher_bot.py not found in src/core/"
    echo "Please run this script from the Data-factory directory"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs
mkdir -p databases

# Check if requirements.txt exists and install dependencies if needed
if [ -f "config/requirements.txt" ]; then
    echo "📦 Checking and installing dependencies..."
    pip3 install -r config/requirements.txt
    if [ $? -ne 0 ]; then
        echo "❌ Error: Failed to install dependencies"
        exit 1
    fi
else
    echo "⚠️ Warning: config/requirements.txt not found, proceeding without dependency check"
fi

echo "🗄️ Database will be created at: databases/crypto_data_$(date +%Y%m%d_%H%M%S).db"
echo "📝 Logs will be written to: logs/bot.log"

# Start the bot
echo "🎯 Starting the bot..."
python3 src/core/crypto_data_fetcher_bot.py

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo "✅ Bot completed successfully"
else
    echo "❌ Bot exited with error code: $exit_code"
fi

echo "👋 Data Fetcher Bot session ended"
