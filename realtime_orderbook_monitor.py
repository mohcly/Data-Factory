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
from typing import Dict, List, Set, Optional
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

    def __init__(self, symbols: List[str], output_dir: str = "data/realtime_orderbooks",
                 duration_hours: Optional[int] = None, verbose: bool = False,
                 depth: int = 20, update_interval: int = 1000):
        """
        Initialize the orderbook monitor.

        Args:
            symbols: List of cryptocurrency symbols to monitor (e.g., ['BTC', 'ETH'])
            output_dir: Directory to save orderbook data
            duration_hours: Optional monitoring duration in hours
            verbose: Enable verbose logging
            depth: Orderbook depth to capture (default: 20 levels)
            update_interval: Update interval in milliseconds (default: 1000ms)
        """
        self.symbols = [s.upper() for s in symbols]
        self.output_dir = Path(output_dir)
        self.duration_hours = duration_hours
        self.verbose = verbose
        self.depth = depth
        self.update_interval = update_interval

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
        # For multiple symbols, we'll create separate connections
        # Binance allows up to 1024 concurrent connections per IP

        tasks = []
        for symbol in self.symbols:
            task = asyncio.create_task(self.monitor_symbol(symbol))
            tasks.append(task)

        logger.info(f"Starting orderbook monitoring for {len(self.symbols)} symbols")

        try:
            # Wait for all monitoring tasks
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in WebSocket monitoring: {e}")

    async def monitor_symbol(self, symbol: str):
        """Monitor orderbook for a single symbol."""
        stream_name = f"{symbol.lower()}usdt@depth{self.depth}@100ms"
        ws_url = f"{self.ws_base_url}{stream_name}"

        logger.info(f"Connecting to {symbol} orderbook stream: {ws_url}")

        retry_count = 0
        max_retries = 5

        while self.running and retry_count < max_retries:
            try:
                async with websockets.connect(ws_url) as ws:
                    logger.info(Colors.success(f"✅ Connected to {symbol} orderbook stream"))
                    self.connected = True

                    while self.running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            await self.process_message(message, symbol)
                        except asyncio.TimeoutError:
                            # Timeout is normal, just continue
                            continue
                        except Exception as e:
                            logger.error(f"Error receiving {symbol} message: {e}")
                            break

            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"{symbol} WebSocket connection closed, retrying...")
            except Exception as e:
                logger.error(f"WebSocket connection error for {symbol}: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                else:
                    logger.error(f"Max retries reached for {symbol}, giving up")

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

            # Handle Binance Futures orderbook depth updates
            # The data comes directly in the message, not wrapped in 'stream'/'data'
            if data.get('e') == 'depthUpdate':
                await self.process_orderbook_update(symbol, data)
            else:
                # Log unexpected message types
                if self.verbose:
                    logger.debug(f"Received {symbol} message type: {data.get('e', 'unknown')}")

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse {symbol} WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing {symbol} message: {e}")

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

            # Periodic save to file (every 2000 updates or every 10 minutes) - Reduced frequency to save space
            total_updates = sum(len(data) for data in self.orderbook_data.values())
            current_time = datetime.now()

            # Save based on update count OR time interval
            should_save = (
                total_updates % 2000 == 0 or  # Every 2000 updates
                (hasattr(self, 'last_save_time') and
                 (current_time - self.last_save_time).seconds >= 600)  # Every 10 minutes
            )

            if should_save:
                self.last_save_time = current_time
                await self.save_data_to_files()

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

                # Color coding based on activity
                if stats['updates'] > 100:
                    count_display = Colors.success(f"{stats['updates']:6d}")
                elif stats['updates'] > 10:
                    count_display = Colors.data(f"{stats['updates']:6d}")
                else:
                    count_display = Colors.warning(f"{stats['updates']:6d}")

                time_display = Colors.success(time_since_update) if last_update and (datetime.now() - last_update).seconds < 10 else Colors.warning(time_since_update)

                print(f"  {Colors.info(symbol):6} {count_display} Last update: {time_display}")

            print(f"\n📁 Data saved to: {Colors.data(str(self.output_dir))}")
            print(Colors.info("💡 Press Ctrl+C to stop monitoring"))
            print(Colors.header("="*70))

    async def save_data_to_files(self):
        """Save new orderbook data to CSV files (append mode) and create compressed backups."""
        try:
            for symbol in self.symbols:
                if not self.orderbook_data[symbol]:
                    continue

                # Create symbol directory
                symbol_dir = self.output_dir / symbol.lower()
                symbol_dir.mkdir(exist_ok=True)

                # Save to CSV (append mode)
                csv_file = symbol_dir / f"{symbol.lower()}_orderbook_realtime.csv"

                # Filter data that's newer than the last saved timestamp
                last_saved = self.last_saved_timestamps[symbol]
                if last_saved:
                    # Only save data newer than what was last saved
                    original_count = len(self.orderbook_data[symbol])
                    new_data = [entry for entry in self.orderbook_data[symbol]
                              if entry['timestamp'] > last_saved]
                    logger.debug(f"📊 {symbol}: {original_count} total records, {len(new_data)} new records since {last_saved}")
                else:
                    # First time saving, save all data
                    new_data = self.orderbook_data[symbol]
                    logger.info(f"📊 {symbol}: First time saving {len(new_data)} records")

                if new_data:
                    # Convert new data to DataFrame
                    df_data = []
                    for entry in new_data:
                        row = {
                            'timestamp': entry['timestamp'],
                            'symbol': entry['symbol'],
                            'first_update_id': entry['first_update_id'],
                            'last_update_id': entry['last_update_id'],
                            'best_bid': entry.get('best_bid', 0),
                            'best_ask': entry.get('best_ask', 0),
                            'spread': entry.get('spread', 0),
                            'mid_price': entry.get('mid_price', 0),
                            'spread_pct': entry.get('spread_pct', 0),
                            'bid_volume_top10': entry.get('bid_volume_top10', 0),
                            'ask_volume_top10': entry.get('ask_volume_top10', 0),
                            'total_volume_top10': entry.get('total_volume_top10', 0),
                            'event_time': entry['event_time'],
                            'transaction_time': entry['transaction_time']
                        }
                        df_data.append(row)

                    df = pd.DataFrame(df_data)

                    # Check if file exists to determine append mode
                    file_exists = csv_file.exists()
                    if file_exists:
                        # Append without header
                        df.to_csv(csv_file, mode='a', header=False, index=False)
                    else:
                        # Create new file with header
                        df.to_csv(csv_file, index=False)

                    # Update last saved timestamp
                    if new_data:
                        self.last_saved_timestamps[symbol] = max(entry['timestamp'] for entry in new_data)

                    logger.info(f"💾 Appended orderbook data: {csv_file} ({len(df_data)} new updates)")

                    # Create compressed backup every hour
                    now = datetime.now()
                    if now.minute == 0:  # Top of the hour
                        compressed_file = symbol_dir / f"{symbol.lower()}_orderbook_{now.strftime('%Y%m%d_%H')}.csv.gz"
                        # For compressed files, we save all current data (not just new)
                        all_df_data = []
                        for entry in self.orderbook_data[symbol]:
                            row = {
                                'timestamp': entry['timestamp'],
                                'symbol': entry['symbol'],
                                'first_update_id': entry['first_update_id'],
                                'last_update_id': entry['last_update_id'],
                                'best_bid': entry.get('best_bid', 0),
                                'best_ask': entry.get('best_ask', 0),
                                'spread': entry.get('spread', 0),
                                'mid_price': entry.get('mid_price', 0),
                                'spread_pct': entry.get('spread_pct', 0),
                                'bid_volume_top10': entry.get('bid_volume_top10', 0),
                                'ask_volume_top10': entry.get('ask_volume_top10', 0),
                                'total_volume_top10': entry.get('total_volume_top10', 0),
                                'event_time': entry['event_time'],
                                'transaction_time': entry['transaction_time']
                            }
                            all_df_data.append(row)

                        if all_df_data:
                            all_df = pd.DataFrame(all_df_data)
                            with gzip.open(compressed_file, 'wt') as f:
                                all_df.to_csv(f, index=False)
                            logger.info(f"📦 Saved compressed orderbook data: {compressed_file}")

            # Memory cleanup - keep only last 500 records per symbol to prevent memory bloat
            for symbol in self.symbols:
                if len(self.orderbook_data[symbol]) > 500:
                    # Keep only the most recent 500 records
                    self.orderbook_data[symbol] = self.orderbook_data[symbol][-500:]
                    logger.debug(f"🧹 Cleaned memory for {symbol}: kept last 500 records")

        except Exception as e:
            logger.error(f"Error saving orderbook data: {e}")

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
        for symbol in self.symbols:
            csv_file = self.output_dir / symbol.lower() / f"{symbol.lower()}_orderbook_realtime.csv"
            if csv_file.exists():
                size_mb = csv_file.stat().st_size / (1024 * 1024)
                size_display = Colors.success(f"{size_mb:.1f} MB") if size_mb > 1 else Colors.data(f"{size_mb:.1f} MB")
                print(f"   {Colors.info(symbol):6}: {size_display}")
                files_found += 1

        if files_found == 0:
            print(Colors.warning("   No data files were created"))

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

            # Connect to all symbols and monitor them
            await self.connect_websocket()

        except Exception as e:
            logger.error(f"Monitor error: {e}")
        finally:
            # Final save of all data
            await self.save_data_to_files()
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
    parser.add_argument('--depth', type=int, default=20,
                       help='Orderbook depth to capture (default: 20)')
    parser.add_argument('--update-interval', type=int, default=100,
                       help='Update interval in milliseconds (default: 100ms)')

    args = parser.parse_args()

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
        print("  python realtime_orderbook_monitor.py --symbols BTC --depth 50")
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
        update_interval=args.update_interval
    )

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
