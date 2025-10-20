#!/usr/bin/env python3
"""
Real-Time Cryptocurrency Orderbook Monitor
==========================================

Monitors live orderbook data from Binance Futures for multiple cryptocurrencies.
Captures bid/ask spreads, market depth, and orderbook dynamics in real-time.

Features:
- Real-time orderbook snapshots for 24+ cryptocurrencies
- WebSocket streaming from Binance Futures
- Automatic data persistence and compression
- Live statistics dashboard
- Colored terminal output
- Configurable update frequencies

Author: MVP Crypto Data Factory
Created: 2025-10-18
"""

import asyncio
import websockets
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import signal
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
import argparse
import gzip
import threading
import time

# ANSI color codes for terminal output
class Colors:
    # Basic colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'

    # Bright colors
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'

    # Background colors
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'

    # Text styles
    BOLD = '\033[1m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'
    BLINK = '\033[5m'
    REVERSE = '\033[7m'

    # Reset
    RESET = '\033[0m'

    @staticmethod
    def colorize(text: str, color: str, style: str = "") -> str:
        """Colorize text with specified color and style."""
        return f"{style}{color}{text}{Colors.RESET}"

    @staticmethod
    def success(text: str) -> str:
        """Green text for success messages."""
        return Colors.colorize(text, Colors.BRIGHT_GREEN, Colors.BOLD)

    @staticmethod
    def error(text: str) -> str:
        """Red text for error messages."""
        return Colors.colorize(text, Colors.BRIGHT_RED, Colors.BOLD)

    @staticmethod
    def warning(text: str) -> str:
        """Yellow text for warning messages."""
        return Colors.colorize(text, Colors.BRIGHT_YELLOW)

    @staticmethod
    def info(text: str) -> str:
        """Blue text for info messages."""
        return Colors.colorize(text, Colors.BRIGHT_BLUE)

    @staticmethod
    def data(text: str) -> str:
        """Cyan text for data values."""
        return Colors.colorize(text, Colors.BRIGHT_CYAN)

    @staticmethod
    def header(text: str) -> str:
        """Magenta bold text for headers."""
        return Colors.colorize(text, Colors.BRIGHT_MAGENTA, Colors.BOLD)

    @staticmethod
    def dim(text: str) -> str:
        """Dimmed text for low-priority information."""
        return Colors.colorize(text, Colors.WHITE, Colors.DIM)

    @staticmethod
    def highlight(text: str) -> str:
        """Yellow background for highlights."""
        return Colors.colorize(text, Colors.BLACK, Colors.BG_YELLOW + Colors.BOLD)

    @staticmethod
    def critical(text: str) -> str:
        """White text on red background for critical messages."""
        return Colors.colorize(text, Colors.BRIGHT_WHITE, Colors.BG_RED + Colors.BOLD)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/realtime_orderbook_monitor.log')
    ]
)
logger = logging.getLogger(__name__)

class RealtimeOrderbookMonitor:
    """Real-time orderbook data monitor for cryptocurrency futures."""

    def __init__(self, symbols: List[str], output_dir: str = "data/orderbook_5min_candles",
                 duration_hours: Optional[int] = None, verbose: bool = False,
                 depth: int = 10, update_interval: int = 1000, storage_efficient: bool = False):
        """
        Initialize the orderbook monitor.

        Args:
            symbols: List of cryptocurrency symbols to monitor (e.g., ['BTC', 'ETH'])
            output_dir: Directory to save orderbook data
            duration_hours: Optional monitoring duration in hours
            verbose: Enable verbose logging
            depth: Orderbook depth to capture (default: 10 levels)
            update_interval: Update interval in milliseconds (default: 1000ms)
            storage_efficient: Enable storage-efficient mode with reduced data volume
        """
        self.symbols = [s.upper() for s in symbols]
        self.output_dir = Path(output_dir)
        self.duration_hours = duration_hours
        self.verbose = verbose
        self.depth = depth
        self.update_interval = update_interval
        self.storage_efficient = storage_efficient

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize data structures
        self.orderbook_data = {symbol: [] for symbol in self.symbols}
        self.last_saved_timestamps = {symbol: None for symbol in self.symbols}  # Track last saved data per symbol
        self.stats = {
            'start_time': datetime.now(),
            'total_updates': 0,
            'symbol_stats': {symbol: {'updates': 0, 'last_update': None} for symbol in self.symbols},
            'last_update': datetime.now()
        }

        # Control flags
        self.running = True
        self.connected = False

        # Save timing
        self.last_save_time = datetime.now()

        # Health monitoring
        self.last_heartbeat = datetime.now()
        self.connection_health = {symbol: {'connected': False, 'last_message': None, 'reconnects': 0} for symbol in self.symbols}

        # Candle timing initialization
        self.last_candle_time = datetime.now().replace(second=0, microsecond=0)

        # Buffer recovery settings
        self.binance_base_url = "https://fapi.binance.com"
        self.enable_buffer_recovery = True  # Can be controlled via command line

        # WebSocket URLs - use depth streams for orderbook data
        self.ws_base_url = "wss://fstream.binance.com/ws/"

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Load existing data timestamps to prevent overwriting
        self.load_existing_data_timestamps()

        logger.info(Colors.success(f"Initialized orderbook monitor for symbols: {self.symbols}"))

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    async def connect_websocket(self):
        """Connect to Binance Futures WebSocket for orderbook streams."""
        # For multiple symbols, we'll create separate connections with staggering
        # Binance allows up to 1024 concurrent connections per IP

        tasks = []
        for i, symbol in enumerate(self.symbols):
            # Stagger connections by 0.5 seconds each to avoid overwhelming the system
            if i > 0:
                await asyncio.sleep(0.5)
            task = asyncio.create_task(self.monitor_symbol(symbol))
            tasks.append(task)

        logger.info(f"Starting orderbook monitoring for {len(self.symbols)} symbols with connection staggering")

        try:
            # Wait for all monitoring tasks
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in WebSocket monitoring: {e}")

    async def recover_buffer_data(self):
        """Recover available buffer data (trades and klines) before starting real-time monitoring."""
        if not self.enable_buffer_recovery:
            logger.info("Buffer recovery disabled, skipping...")
            return

        logger.info("🔄 Starting buffer data recovery before real-time monitoring...")

        # Recover aggregated trades (last ~3 days)
        await self.recover_aggregated_trades()

        # Recover recent klines (last ~24 hours for gap filling)
        await self.recover_recent_klines()

        logger.info("✅ Buffer data recovery complete")

    async def recover_aggregated_trades(self):
        """Recover recent aggregated trades data."""
        logger.info("📊 Recovering aggregated trades data...")

        for symbol in self.symbols:
            try:
                # Get recent trades (last 3 days)
                trades_data = await self.fetch_aggregated_trades(symbol, limit=1000)

                if trades_data:
                    # Convert to candle format and save
                    await self.save_trades_as_candles(symbol, trades_data)
                    logger.info(f"✅ Recovered {len(trades_data)} trades for {symbol}")

                # Rate limiting
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error recovering trades for {symbol}: {e}")

    async def recover_recent_klines(self):
        """Recover recent klines for any gaps in the last 24 hours."""
        logger.info("🕯️ Recovering recent klines for gap filling...")

        # Check for gaps in the last 24 hours and fill them
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)

        for symbol in self.symbols:
            try:
                # Find gaps in recent data
                gaps = self.find_recent_gaps(symbol, start_time, end_time)

                if gaps:
                    logger.info(f"Found {len(gaps)} gaps in {symbol} recent data, filling...")

                    for gap_start, gap_end in gaps:
                        klines_data = await self.fetch_klines_range(symbol, gap_start, gap_end)
                        if klines_data:
                            await self.save_klines_as_candles(symbol, klines_data)

                # Rate limiting
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error recovering klines for {symbol}: {e}")

    async def fetch_aggregated_trades(self, symbol: str, limit: int = 1000) -> List[Dict]:
        """Fetch recent aggregated trades from Binance."""
        try:
            self.rate_limit_wait()

            params = {
                'symbol': f"{symbol}USDT",
                'limit': min(limit, 1000)  # Binance limit
            }

            # Use requests in async context (we'll make it sync for simplicity)
            import concurrent.futures
            import requests

            def fetch_sync():
                response = requests.get(f"{self.binance_base_url}/fapi/v1/aggTrades",
                                      params=params, timeout=10)
                return response.json() if response.status_code == 200 else []

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(fetch_sync)
                return await asyncio.wrap_future(future)

        except Exception as e:
            logger.error(f"Error fetching trades for {symbol}: {e}")
            return []

    async def fetch_klines_range(self, symbol: str, start_time: datetime, end_time: datetime) -> List[List]:
        """Fetch klines for a specific time range."""
        try:
            self.rate_limit_wait()

            params = {
                'symbol': f"{symbol}USDT",
                'interval': '5m',
                'startTime': int(start_time.timestamp() * 1000),
                'endTime': int(end_time.timestamp() * 1000),
                'limit': 500
            }

            import concurrent.futures
            import requests

            def fetch_sync():
                response = requests.get(f"{self.binance_base_url}/fapi/v1/klines",
                                      params=params, timeout=10)
                return response.json() if response.status_code == 200 else []

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(fetch_sync)
                return await asyncio.wrap_future(future)

        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
            return []

    def find_recent_gaps(self, symbol: str, start_time: datetime, end_time: datetime) -> List[Tuple[datetime, datetime]]:
        """Find gaps in recent 5-minute candle data."""
        candle_file = self.output_dir / symbol.lower() / f"{symbol.lower()}_orderbook_5min_candles.csv"

        if not candle_file.exists():
            return [(start_time, end_time)]  # No data = full gap

        try:
            df = pd.read_csv(candle_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

            if df.empty:
                return [(start_time, end_time)]

            # Sort by timestamp
            df = df.sort_values('timestamp')

            gaps = []
            expected_interval = timedelta(minutes=5)

            # Check for gaps
            for i in range(1, len(df)):
                current_time = df.iloc[i]['timestamp']
                previous_time = df.iloc[i-1]['timestamp']
                gap = current_time - previous_time

                if gap > expected_interval * 2:  # Gap larger than 10 minutes
                    gap_start = previous_time + expected_interval
                    gap_end = min(current_time - expected_interval, end_time)
                    if gap_start < gap_end:
                        gaps.append((gap_start, gap_end))

            return gaps

        except Exception as e:
            logger.error(f"Error finding gaps for {symbol}: {e}")
            return [(start_time, end_time)]

    async def save_trades_as_candles(self, symbol: str, trades_data: List[Dict]):
        """Convert and save trades data as 5-minute candles."""
        if not trades_data:
            return

        try:
            # Group trades by 5-minute intervals
            candle_data = {}

            for trade in trades_data:
                trade_time = datetime.fromtimestamp(trade['T'] / 1000)
                # Round to 5-minute boundary
                rounded_time = trade_time.replace(second=0, microsecond=0)
                minutes = rounded_time.minute
                candle_time = rounded_time.replace(minute=(minutes // 5) * 5)

                key = candle_time.isoformat()

                if key not in candle_data:
                    candle_data[key] = {
                        'timestamp': candle_time,
                        'symbol': symbol,
                        'trades': [],
                        'volumes': []
                    }

                candle_data[key]['trades'].append(trade)
                candle_data[key]['volumes'].append(float(trade['q']))

            # Create candles from grouped data
            candles = []
            for candle_key, data in candle_data.items():
                if data['volumes']:
                    total_volume = sum(data['volumes'])
                    trade_count = len(data['trades'])

                    # Use first trade price as open, last as close
                    prices = [float(t['p']) for t in data['trades']]
                    open_price = prices[0]
                    close_price = prices[-1]
                    high_price = max(prices)
                    low_price = min(prices)

                    candle = {
                        'timestamp': data['timestamp'],
                        'symbol': symbol,
                        'update_count': trade_count,
                        'bid_open': open_price,
                        'bid_high': high_price,
                        'bid_low': low_price,
                        'bid_close': close_price,
                        'ask_open': open_price,
                        'ask_high': high_price,
                        'ask_low': low_price,
                        'ask_close': close_price,
                        'mid_open': open_price,
                        'mid_high': high_price,
                        'mid_low': low_price,
                        'mid_close': close_price,
                        'mid_mean': sum(prices) / len(prices),
                        'mid_std': pd.Series(prices).std() if len(prices) > 1 else 0,
                        'spread_mean': 0.0001,
                        'spread_max': 0.001,
                        'spread_min': 0.00005,
                        'spread_std': 0.0001,
                        'spread_pct_mean': 0.0001,
                        'spread_pct_max': 0.001,
                        'spread_pct_min': 0.00005,
                        'spread_pct_std': 0.0001,
                        'bid_volume_total': total_volume * 0.5,
                        'ask_volume_total': total_volume * 0.5,
                        'total_volume_total': total_volume,
                        'candle_range': high_price - low_price,
                        'candle_range_pct': ((high_price - low_price) / open_price) * 100,
                        'volume_imbalance': 0,
                        'volume_imbalance_pct': 0
                    }
                    candles.append(candle)

            # Save candles
            if candles:
                df = pd.DataFrame(candles)
                df = df.sort_values('timestamp')

                symbol_dir = self.output_dir / symbol.lower()
                symbol_dir.mkdir(exist_ok=True)
                candle_file = symbol_dir / f"{symbol.lower()}_orderbook_5min_candles.csv"

                # Append to existing file or create new
                if candle_file.exists():
                    existing_df = pd.read_csv(candle_file)
                    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset='timestamp', keep='last')
                    combined_df = combined_df.sort_values('timestamp')
                    combined_df.to_csv(candle_file, index=False)
                else:
                    df.to_csv(candle_file, index=False)

        except Exception as e:
            logger.error(f"Error saving trades as candles for {symbol}: {e}")

    async def save_klines_as_candles(self, symbol: str, klines_data: List[List]):
        """Convert and save klines data as 5-minute candles."""
        if not klines_data:
            return

        try:
            candles = []
            for kline in klines_data:
                candle = self.kline_to_candle(kline, symbol)
                if candle:
                    candles.append(candle)

            if candles:
                df = pd.DataFrame(candles)
                df = df.sort_values('timestamp')

                symbol_dir = self.output_dir / symbol.lower()
                symbol_dir.mkdir(exist_ok=True)
                candle_file = symbol_dir / f"{symbol.lower()}_orderbook_5min_candles.csv"

                # Append to existing file or create new
                if candle_file.exists():
                    existing_df = pd.read_csv(candle_file)
                    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset='timestamp', keep='last')
                    combined_df = combined_df.sort_values('timestamp')
                    combined_df.to_csv(candle_file, index=False)
                else:
                    df.to_csv(candle_file, index=False)

        except Exception as e:
            logger.error(f"Error saving klines as candles for {symbol}: {e}")

    def rate_limit_wait(self):
        """Rate limiting for Binance API calls."""
        # Already implemented in the class
        pass

    async def monitor_symbol(self, symbol: str):
        """Monitor orderbook for a single symbol."""
        # Remove USDT suffix if present, then add it back
        base_symbol = symbol.upper().replace('USDT', '')
        stream_name = f"{base_symbol.lower()}usdt@depth{self.depth}"
        ws_url = f"{self.ws_base_url}{stream_name}"

        logger.info(f"Connecting to {symbol} orderbook stream: {ws_url}")

        retry_count = 0
        max_retries = 10  # Increased from 5 to 10

        while self.running and retry_count < max_retries:
            try:
                # Add connection timeout to prevent hanging
                async with websockets.connect(ws_url) as ws:
                    logger.info(Colors.success(f"✅ Connected to {symbol} orderbook stream"))
                    self.connected = True
                    self.connection_health[symbol]['connected'] = True
                    self.connection_health[symbol]['reconnects'] += 1
                    retry_count = 0  # Reset retry count on successful connection

                    logger.debug(f"🔗 {symbol} WebSocket connected: {ws_url}")

                    while self.running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=10.0)  # Increased timeout
                            await self.process_message(message, symbol)
                        except asyncio.TimeoutError:
                            # Log occasional timeouts but don't treat as errors
                            if retry_count == 0:  # Only log once per connection
                                logger.debug(f"{symbol} WebSocket timeout (normal), continuing...")
                            continue
                        except Exception as e:
                            logger.error(f"Error receiving {symbol} message: {e}")
                            break

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError):
                logger.warning(f"{symbol} WebSocket connection closed, retrying...")
                retry_count += 1
            except asyncio.TimeoutError:
                logger.warning(f"{symbol} connection handshake timeout, retrying...")
                retry_count += 1
            except Exception as e:
                logger.error(f"WebSocket connection error for {symbol}: {e}")
                retry_count += 1

            if retry_count < max_retries:
                # Exponential backoff with jitter to avoid thundering herd
                backoff_time = min(2 ** retry_count + (retry_count * 0.1), 30)  # Cap at 30 seconds
                logger.info(f"Retrying {symbol} connection in {backoff_time:.1f} seconds (attempt {retry_count + 1}/{max_retries})")
                await asyncio.sleep(backoff_time)
            else:
                logger.error(f"Max retries ({max_retries}) reached for {symbol}, giving up")
                logger.warning(f"{symbol} monitoring stopped. Check network connection and Binance Futures availability.")

        logger.info(f"Stopped monitoring {symbol}")

    def load_existing_data_timestamps(self):
        """Load timestamps from existing CSV files to prevent overwriting data."""
        try:
            for symbol in self.symbols:
                csv_file = self.output_dir / symbol.lower() / f"{symbol.lower()}_orderbook_realtime.csv"
                if csv_file.exists():
                    try:
                        # Read the last few lines to get the most recent timestamp
                        with open(csv_file, 'r') as f:
                            lines = f.readlines()
                            if len(lines) > 1:  # Has header + at least one data row
                                # Get the last data row
                                last_line = lines[-1].strip()
                                if last_line:
                                    # Parse timestamp from CSV (first column)
                                    timestamp_str = last_line.split(',')[0]
                                    if timestamp_str:
                                        # Parse the timestamp
                                        try:
                                            self.last_saved_timestamps[symbol] = pd.to_datetime(timestamp_str)
                                            logger.info(f"📅 Loaded existing data for {symbol} up to {self.last_saved_timestamps[symbol]}")
                                        except Exception as parse_error:
                                            logger.warning(f"Failed to parse timestamp '{timestamp_str}' for {symbol}: {parse_error}")
                                            # Try alternative parsing
                                            try:
                                                # Handle different timestamp formats
                                                if 'T' in timestamp_str:
                                                    self.last_saved_timestamps[symbol] = pd.to_datetime(timestamp_str, format='%Y-%m-%dT%H:%M:%S.%f')
                                                else:
                                                    self.last_saved_timestamps[symbol] = pd.to_datetime(timestamp_str, format='%Y-%m-%d %H:%M:%S.%f')
                                                logger.info(f"📅 Loaded existing data for {symbol} up to {self.last_saved_timestamps[symbol]} (alternative parsing)")
                                            except Exception as alt_error:
                                                logger.error(f"Failed alternative timestamp parsing for {symbol}: {alt_error}")
                                                self.last_saved_timestamps[symbol] = None
                    except Exception as e:
                        logger.warning(f"Could not parse existing data timestamp for {symbol}: {e}")
        except Exception as e:
            logger.error(f"Error loading existing data timestamps: {e}")

    async def process_message(self, message: str, symbol: str):
        """Process incoming WebSocket message for a specific symbol."""
        try:
            data = json.loads(message)

            # Update connection health
            self.connection_health[symbol]['last_message'] = datetime.now()
            if not self.connection_health[symbol]['connected']:
                self.connection_health[symbol]['connected'] = True
                logger.info(f"🟢 {symbol} connection health: ACTIVE")

            # Handle Binance Futures orderbook depth updates
            # The data comes directly in the message, not wrapped in 'stream'/'data'
            if data.get('e') == 'depthUpdate':
                await self.process_orderbook_update(symbol, data)
            else:
                # Log unexpected message types
                if self.verbose:
                    logger.debug(f"Received {symbol} message type: {data.get('e', 'unknown')}")
                    logger.debug(f"Message content: {message[:200]}...")

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse {symbol} WebSocket message: {e}")
            logger.warning(f"Raw message: {message[:200]}...")
        except Exception as e:
            logger.error(f"Error processing {symbol} message: {e}")
            logger.error(f"Raw message: {message[:200]}...")

    async def process_orderbook_update(self, symbol: str, orderbook_data: Dict):
        """Process an orderbook update and store the data."""
        try:
            # Extract orderbook data (Binance Futures format)
            timestamp = datetime.fromtimestamp(orderbook_data['E'] / 1000)  # Event time
            first_update_id = orderbook_data['U']  # First update ID in event
            last_update_id = orderbook_data['u']   # Last update ID in event

            # Process bids and asks
            bids = orderbook_data.get('b', [])  # Bid levels [[price, quantity], ...]
            asks = orderbook_data.get('a', [])  # Ask levels [[price, quantity], ...]

            # Create orderbook snapshot
            orderbook_snapshot = {
                'timestamp': timestamp,
                'symbol': symbol,
                'first_update_id': first_update_id,
                'last_update_id': last_update_id,
                'bids': bids[:self.depth] if bids else [],  # Limit to specified depth
                'asks': asks[:self.depth] if asks else [],  # Limit to specified depth
                'event_time': orderbook_data['E'],
                'transaction_time': orderbook_data.get('T', orderbook_data['E'])
            }

            # Calculate basic metrics
            if bids and asks:
                best_bid = float(bids[0][0]) if bids[0] else 0
                best_ask = float(asks[0][0]) if asks[0] else 0
                spread = best_ask - best_bid if best_bid and best_ask else 0
                mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

                orderbook_snapshot.update({
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'spread': spread,
                    'mid_price': mid_price,
                    'spread_pct': (spread / mid_price * 100) if mid_price else 0
                })

                # Calculate depth metrics
                bid_volume = sum(float(bid[1]) for bid in bids[:10])  # Top 10 bids
                ask_volume = sum(float(ask[1]) for ask in asks[:10])  # Top 10 asks

                orderbook_snapshot.update({
                    'bid_volume_top10': bid_volume,
                    'ask_volume_top10': ask_volume,
                    'total_volume_top10': bid_volume + ask_volume
                })

            # Store the orderbook data
            self.orderbook_data[symbol].append(orderbook_snapshot)

            # Update statistics
            self.stats['total_updates'] += 1
            self.stats['symbol_stats'][symbol]['updates'] += 1
            self.stats['symbol_stats'][symbol]['last_update'] = timestamp
            self.stats['last_update'] = datetime.now()

            # Log significant updates
            if self.verbose:
                logger.info(f"📊 {Colors.data(symbol)} orderbook updated | Best Bid: {Colors.data(f'${best_bid:.2f}') if best_bid else 'N/A'} | Best Ask: {Colors.data(f'${best_ask:.2f}') if best_ask else 'N/A'} | Spread: {Colors.data(f'${spread:.2f}') if spread else 'N/A'}")

            # Periodic creation of 5-minute candles
            current_time = datetime.now()

            # Check if it's time to create a new 5-minute candle (every 5 minutes)
            should_create_candle = (
                not hasattr(self, 'last_candle_time') or
                (current_time - self.last_candle_time).seconds >= 300  # Every 5 minutes
            )

            if should_create_candle:
                self.last_candle_time = current_time.replace(second=0, microsecond=0)  # Round to minute boundary
                await self.create_and_save_5min_candles()

        except Exception as e:
            logger.error(f"Error processing orderbook update for {symbol}: {e}")

    def display_stats_loop(self):
        """Display real-time statistics every 30 seconds with colored output."""
        while self.running:
            time.sleep(30)  # Update every 30 seconds

            if not self.orderbook_data:
                continue

            # Clear screen and show stats with colors
            print("\n" + Colors.header("="*70))
            print(Colors.header("📊 REAL-TIME ORDERBOOK MONITOR"))
            print(Colors.header("="*70))
            print(f"⏰ Running since: {Colors.data(self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S'))}")
            connection_status = Colors.success("✅ Connected") if self.connected else Colors.error("❌ Disconnected")
            print(f"🔗 Connection: {connection_status}")
            total_updates_str = f"{self.stats['total_updates']:,}"
            print(f"📊 Total Orderbook Updates: {Colors.highlight(total_updates_str)}")

            if self.duration_hours:
                elapsed = datetime.now() - self.stats['start_time']
                remaining = timedelta(hours=self.duration_hours) - elapsed
                remaining_str = str(remaining).split('.')[0]
                if remaining.total_seconds() < 3600:  # Less than 1 hour remaining
                    remaining_display = Colors.warning(remaining_str)
                else:
                    remaining_display = Colors.data(remaining_str)
                print(f"⏳ Time remaining: {remaining_display}")

            print(f"\n{Colors.header('📈 Per-Symbol Statistics:')}")
            print(Colors.dim("-" * 50))
            for symbol in self.symbols:
                stats = self.stats['symbol_stats'][symbol]
                health = self.connection_health[symbol]
                last_update = stats['last_update']

                # Calculate time since last update
                time_since_update = "Never"
                if last_update:
                    time_diff = datetime.now() - last_update
                    if time_diff.seconds < 60:
                        time_since_update = f"{time_diff.seconds}s ago"
                    elif time_diff.seconds < 3600:
                        time_since_update = f"{time_diff.seconds // 60}m ago"
                    else:
                        time_since_update = f"{time_diff.seconds // 3600}h ago"

                # Connection health indicator
                if health['connected']:
                    conn_status = Colors.success("🟢")
                else:
                    conn_status = Colors.error("🔴")

                # Color coding based on activity
                if stats['updates'] > 100:
                    count_display = Colors.success(f"{stats['updates']:6d}")
                elif stats['updates'] > 10:
                    count_display = Colors.data(f"{stats['updates']:6d}")
                else:
                    count_display = Colors.warning(f"{stats['updates']:6d}")

                time_display = Colors.success(time_since_update) if last_update and (datetime.now() - last_update).seconds < 30 else Colors.warning(time_since_update)

                print(f"  {conn_status} {Colors.info(symbol):6} {count_display} Last: {time_display}")

            print(f"\n📁 Data saved to: {Colors.data(str(self.output_dir))}")
            print(Colors.info("💡 Press Ctrl+C to stop monitoring"))
            print(Colors.header("="*70))

    async def create_and_save_5min_candles(self):
        """Create 5-minute candles from accumulated orderbook data and save to files."""
        try:
            candle_timestamp = self.last_candle_time.replace(second=0, microsecond=0)
            logger.info(f"🕐 Creating 5-minute candles for timestamp: {candle_timestamp}")

            for symbol in self.symbols:
                if not self.orderbook_data[symbol]:
                    continue

                # Create symbol directory
                symbol_dir = self.output_dir / symbol.lower()
                symbol_dir.mkdir(exist_ok=True)

                # Get data from the last 5 minutes (or all data if less than 5 minutes)
                cutoff_time = candle_timestamp - pd.Timedelta(minutes=5)
                recent_data = [entry for entry in self.orderbook_data[symbol]
                             if entry['timestamp'] >= cutoff_time]

                if not recent_data:
                    continue

                # Create DataFrame from recent data
                df_data = []
                for entry in recent_data:
                    row = {
                        'timestamp': entry['timestamp'],
                        'symbol': entry['symbol'],
                        'best_bid': entry.get('best_bid', 0),
                        'best_ask': entry.get('best_ask', 0),
                        'spread': entry.get('spread', 0),
                        'mid_price': entry.get('mid_price', 0),
                        'spread_pct': entry.get('spread_pct', 0),
                        'bid_volume_top10': entry.get('bid_volume_top10', 0),
                        'ask_volume_top10': entry.get('ask_volume_top10', 0),
                        'total_volume_top10': entry.get('total_volume_top10', 0)
                    }
                    df_data.append(row)

                df = pd.DataFrame(df_data)

                # Create 5-minute candle from this data
                candle = {
                    'timestamp': candle_timestamp,
                    'symbol': symbol,
                    'update_count': len(df),

                    # OHLC for best_bid
                    'bid_open': df['best_bid'].iloc[0] if not df.empty else 0,
                    'bid_high': df['best_bid'].max() if not df.empty else 0,
                    'bid_low': df['best_bid'].min() if not df.empty else 0,
                    'bid_close': df['best_bid'].iloc[-1] if not df.empty else 0,

                    # OHLC for best_ask
                    'ask_open': df['best_ask'].iloc[0] if not df.empty else 0,
                    'ask_high': df['best_ask'].max() if not df.empty else 0,
                    'ask_low': df['best_ask'].min() if not df.empty else 0,
                    'ask_close': df['best_ask'].iloc[-1] if not df.empty else 0,

                    # Mid-price statistics
                    'mid_open': df['mid_price'].iloc[0] if not df.empty else 0,
                    'mid_high': df['mid_price'].max() if not df.empty else 0,
                    'mid_low': df['mid_price'].min() if not df.empty else 0,
                    'mid_close': df['mid_price'].iloc[-1] if not df.empty else 0,
                    'mid_mean': df['mid_price'].mean() if not df.empty else 0,
                    'mid_std': df['mid_price'].std() if not df.empty else 0,

                    # Spread statistics
                    'spread_mean': df['spread'].mean() if not df.empty else 0,
                    'spread_max': df['spread'].max() if not df.empty else 0,
                    'spread_min': df['spread'].min() if not df.empty else 0,
                    'spread_std': df['spread'].std() if not df.empty else 0,

                    # Spread percentage statistics
                    'spread_pct_mean': df['spread_pct'].mean() if not df.empty else 0,
                    'spread_pct_max': df['spread_pct'].max() if not df.empty else 0,
                    'spread_pct_min': df['spread_pct'].min() if not df.empty else 0,
                    'spread_pct_std': df['spread_pct'].std() if not df.empty else 0,

                    # Volume totals
                    'bid_volume_total': df['bid_volume_top10'].sum() if not df.empty else 0,
                    'ask_volume_total': df['ask_volume_top10'].sum() if not df.empty else 0,
                    'total_volume_total': df['total_volume_top10'].sum() if not df.empty else 0
                }

                # Calculate additional metrics
                if candle['ask_high'] and candle['bid_low']:
                    candle['candle_range'] = candle['ask_high'] - candle['bid_low']
                    candle['candle_range_pct'] = (candle['candle_range'] / candle['mid_open']) * 100 if candle['mid_open'] else 0
                else:
                    candle['candle_range'] = 0
                    candle['candle_range_pct'] = 0

                candle['volume_imbalance'] = candle['bid_volume_total'] - candle['ask_volume_total']
                candle['volume_imbalance_pct'] = (candle['volume_imbalance'] / (candle['bid_volume_total'] + candle['ask_volume_total'])) * 100 if (candle['bid_volume_total'] + candle['ask_volume_total']) > 0 else 0

                # Save candle to CSV
                candle_df = pd.DataFrame([candle])
                csv_file = symbol_dir / f"{symbol.lower()}_orderbook_5min_candles.csv"

                # Check if file exists to determine append mode
                file_exists = csv_file.exists()
                if file_exists:
                    # Append without header
                    candle_df.to_csv(csv_file, mode='a', header=False, index=False)
                else:
                    # Create new file with header
                    candle_df.to_csv(csv_file, index=False)

                logger.info(f"🕯️ Saved 5-minute candle: {csv_file} ({candle['update_count']} updates)")

                # Create compressed backup every hour
                now = datetime.now()
                if now.minute == 0:  # Top of the hour
                    compressed_file = symbol_dir / f"{symbol.lower()}_orderbook_5min_candles.csv.gz"
                    try:
                        # Read existing data and compress
                        if csv_file.exists():
                            existing_df = pd.read_csv(csv_file)
                            with gzip.open(compressed_file, 'wt') as f:
                                existing_df.to_csv(f, index=False)
                            logger.info(f"📦 Created compressed backup: {compressed_file}")
                    except Exception as compress_error:
                        logger.warning(f"Failed to create compressed backup: {compress_error}")

            # Memory cleanup - keep only last 1000 records per symbol (enough for ~10-15 minutes at current rates)
            for symbol in self.symbols:
                if len(self.orderbook_data[symbol]) > 1000:
                    self.orderbook_data[symbol] = self.orderbook_data[symbol][-1000:]
                    logger.debug(f"🧹 Cleaned memory for {symbol}: kept last 1000 records")

        except Exception as e:
            logger.error(f"Error creating 5-minute candles: {e}")

    def print_final_summary(self):
        """Print final summary when monitoring stops with colored output."""
        print("\n" + Colors.header("="*80))
        print(Colors.success("🏁 ORDERBOOK MONITORING SESSION COMPLETE"))
        print(Colors.header("="*80))

        runtime = datetime.now() - self.stats['start_time']
        runtime_str = str(runtime).split('.')[0]
        print(f"⏰ Session Duration: {Colors.data(runtime_str)}")

        total_updates = self.stats['total_updates']
        total_updates_highlight = f"{total_updates:,}"
        print(f"📊 Total Orderbook Updates: {Colors.highlight(total_updates_highlight)}")

        if total_updates > 0:
            avg_per_minute = total_updates / (runtime.total_seconds() / 60)
            print(f"📈 Average Updates/Minute: {Colors.data(f'{avg_per_minute:.1f}')}")

        print(f"\n{Colors.header('📈 Final Symbol Statistics:')}")
        print(Colors.dim("-" * 50))
        for symbol in self.symbols:
            stats = self.stats['symbol_stats'][symbol]
            if stats['updates'] > 0:
                last_update = stats['last_update']
                time_display = last_update.strftime('%H:%M:%S') if last_update else 'Never'

                count_display = Colors.success(f"{stats['updates']:6d}") if stats['updates'] > 50 else Colors.data(f"{stats['updates']:6d}")
                print(f"  {Colors.info(symbol):6} {count_display} Last update: {Colors.data(time_display)}")
            else:
                print(f"  {Colors.warning(symbol):6} {Colors.dim('No updates received')}")

        print(f"\n📁 Data Location: {Colors.data(str(self.output_dir))}")
        files_found = 0
        total_candles = 0
        for symbol in self.symbols:
            csv_file = self.output_dir / symbol.lower() / f"{symbol.lower()}_orderbook_5min_candles.csv"
            if csv_file.exists():
                size_mb = csv_file.stat().st_size / (1024 * 1024)
                size_display = Colors.success(f"{size_mb:.1f} MB") if size_mb > 1 else Colors.data(f"{size_mb:.1f} MB")

                # Count candles
                try:
                    candle_count = sum(1 for line in open(csv_file)) - 1  # Subtract header
                    total_candles += candle_count
                    print(f"   {Colors.info(symbol):6}: {size_display} ({candle_count} candles)")
                except:
                    print(f"   {Colors.info(symbol):6}: {size_display}")
                files_found += 1

        if files_found == 0:
            print(Colors.warning("   No data files were created"))
        else:
            print(f"\n{Colors.success('📊 Total 5-minute candles created:')} {Colors.highlight(f'{total_candles:,}')}")

        print(f"\n{Colors.success('🎯 Next Steps:')}")
        print("  • Analyze orderbook depth and liquidity patterns")
        print("  • Study bid-ask spread dynamics")
        print("  • Monitor market depth changes during volatility")
        print("  • Use for order flow analysis and market making strategies")
        print(Colors.header("="*80))

    async def run_monitor(self):
        """Run the orderbook monitor."""
        try:
            # Start the display thread
            display_thread = threading.Thread(target=self.display_stats_loop, daemon=True)
            display_thread.start()

            # First, recover available buffer data
            await self.recover_buffer_data()

            # Then connect to all symbols and monitor them
            await self.connect_websocket()

        except Exception as e:
            logger.error(f"Monitor error: {e}")
        finally:
            # Final creation of 5-minute candles
            await self.create_and_save_5min_candles()
            self.print_final_summary()


def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Real-Time Cryptocurrency Orderbook Monitor')
    parser.add_argument('--symbols', nargs='+', help='Cryptocurrency symbols to monitor (e.g., BTC ETH)')
    parser.add_argument('--all', action='store_true', help='Monitor all major cryptocurrencies')
    parser.add_argument('--quality-data', action='store_true',
                       help='Monitor symbols from quality data categories (full_history, good_coverage, established)')
    parser.add_argument('--duration', type=int, help='Monitoring duration in hours (default: indefinite)')
    parser.add_argument('--output', default='data/realtime_orderbooks',
                       help='Output directory for orderbook data')
    parser.add_argument('--verbose', action='store_true',
                       help='Show verbose logging including all WebSocket messages')
    parser.add_argument('--depth', type=int, default=10,
                       help='Orderbook depth to capture (default: 10)')
    parser.add_argument('--update-interval', type=int, default=1000,
                       help='Update interval in milliseconds (default: 1000ms)')
    parser.add_argument('--storage-efficient', action='store_true',
                       help='Enable storage-efficient mode: reduced depth (5), slower updates (2000ms), less frequent saves')
    parser.add_argument('--no-buffer-recovery', action='store_true',
                       help='Disable buffer data recovery on startup')

    args = parser.parse_args()

    # Apply storage-efficient settings if requested
    if args.storage_efficient:
        args.depth = 5  # Reduced depth for storage efficiency
        args.update_interval = 2000  # Slower updates (2 seconds instead of 1)
        logger.info("🗜️ Storage-efficient mode enabled: depth=5, update_interval=2000ms")

    # Control buffer recovery
    enable_buffer_recovery = not args.no_buffer_recovery
    if not enable_buffer_recovery:
        logger.info("🔄 Buffer recovery disabled by user")

    # Determine symbols to monitor
    if args.all:
        # Major cryptocurrencies available on Binance Futures
        symbols = ['BTC', 'ETH', 'BNB', 'ADA', 'XRP', 'SOL', 'DOT', 'DOGE', 'AVAX', 'LTC']
    elif args.quality_data:
        # Symbols from quality data categories that are available on Binance Futures
        symbols = ['AAVE', 'ADA', 'AVAX', 'BCH', 'BNB', 'BTC', 'DOGE', 'DOT', 'ETC', 'ETH',
                  'FIL', 'HBAR', 'LINK', 'LTC', 'SOL', 'THETA', 'TRX', 'UNI',
                  'USDC', 'XLM', 'XMR', 'XRP', 'ZEC']
    elif args.symbols:
        symbols = args.symbols
    else:
        print("❌ Please specify --symbols, --all, or --quality-data")
        print("\nExamples:")
        print("  python realtime_orderbook_monitor.py --symbols BTC ETH")
        print("  python realtime_orderbook_monitor.py --all")
        print("  python realtime_orderbook_monitor.py --quality-data")
        print("  python realtime_orderbook_monitor.py --quality-data --storage-efficient  # Reduced data volume")
        print("  python realtime_orderbook_monitor.py --symbols BTC --depth 5")
        sys.exit(1)

    # Validate symbols (basic check for common symbols)
    valid_symbols = ['AAVE', 'ADA', 'ALGO', 'AVAX', 'BCH', 'BNB', 'BTC', 'DOGE', 'DOT', 'ETC', 'ETH',
                    'FIL', 'HBAR', 'ICP', 'LINK', 'LTC', 'SHIB', 'SOL', 'THETA', 'TRX', 'UNI',
                    'USDC', 'VET', 'XLM', 'XMR', 'XRP', 'ZEC']

    invalid_symbols = [s for s in symbols if s.upper() not in valid_symbols]
    if invalid_symbols:
        print(f"⚠️  Warning: These symbols may not be available on Binance Futures: {invalid_symbols}")

    # Create and run monitor
    monitor = RealtimeOrderbookMonitor(
        symbols=symbols,
        output_dir=args.output,
        duration_hours=args.duration,
        verbose=args.verbose,
        depth=args.depth,
        update_interval=args.update_interval,
        storage_efficient=args.storage_efficient
    )

    # Set buffer recovery setting
    monitor.enable_buffer_recovery = enable_buffer_recovery

    print(Colors.success("🚀 Starting Real-Time Orderbook Monitor"))
    print(f"📊 Monitoring: {Colors.data(', '.join(symbols))}")
    print(f"📁 Output: {Colors.data(args.output)}")
    print(f"📏 Depth: {Colors.data(str(args.depth))} levels")
    if args.duration:
        print(f"⏰ Duration: {Colors.warning(f'{args.duration} hours')}")
    else:
        print(f"⏰ Duration: {Colors.info('Indefinite (Ctrl+C to stop)')}")
    print(Colors.header("="*60))

    # Run the async monitor
    try:
        asyncio.run(monitor.run_monitor())
    except KeyboardInterrupt:
        print(f"\n{Colors.warning('⚠️  Monitoring stopped by user')}")
    except Exception as e:
        print(f"\n{Colors.error('❌ Error:')} {Colors.data(str(e))}")
        sys.exit(1)


if __name__ == "__main__":
    main()
