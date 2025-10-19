#!/usr/bin/env python3
"""
Real-Time Liquidation Data Monitor

A comprehensive real-time liquidation data monitoring system for cryptocurrency futures.
Captures live liquidation events from Binance Futures and saves them to structured CSV files.

Features:
- Real-time WebSocket connections to Binance Futures
- Multi-symbol monitoring (BTC, ETH, etc.)
- Automatic reconnection and error handling
- Structured data storage with timestamps
- Live statistics and monitoring dashboard
- Easy terminal operation with simple commands

Usage:
    # Monitor BTC and ETH liquidations
    python realtime_liquidation_monitor.py --symbols BTC ETH

    # Monitor all major cryptocurrencies
    python realtime_liquidation_monitor.py --all

    # Monitor for specific duration (in hours)
    python realtime_liquidation_monitor.py --symbols BTC --duration 24

    # Monitor with custom output directory
    python realtime_liquidation_monitor.py --symbols BTC ETH --output /path/to/output

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
import subprocess
import platform

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

    # Lowercase aliases for convenience
    bold = BOLD
    underline = UNDERLINE
    blink = BLINK
    reverse = REVERSE

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


class AlertSystem:
    """Interactive alert system for liquidation events."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.system = platform.system().lower()

        # Alert configurations for different liquidation types
        self.alerts = {
            'small_long': {
                'sound': 'low_tone',
                'message': 'Small LONG liquidation',
                'color': Colors.warning
            },
            'small_short': {
                'sound': 'low_tone',
                'message': 'Small SHORT liquidation',
                'color': Colors.warning
            },
            'medium_long': {
                'sound': 'medium_tone',
                'message': 'Medium LONG liquidation',
                'color': Colors.info
            },
            'medium_short': {
                'sound': 'medium_tone',
                'message': 'Medium SHORT liquidation',
                'color': Colors.info
            },
            'large_long': {
                'sound': 'high_tone',
                'message': 'LARGE LONG liquidation',
                'color': Colors.error
            },
            'large_short': {
                'sound': 'high_tone',
                'message': 'LARGE SHORT liquidation',
                'color': Colors.error
            },
            'huge_long': {
                'sound': 'urgent_tone',
                'message': 'HUGE LONG liquidation',
                'color': Colors.critical
            },
            'huge_short': {
                'sound': 'urgent_tone',
                'message': 'HUGE SHORT liquidation',
                'color': Colors.critical
            }
        }

    def get_alert_type(self, side: str, amount: float) -> str:
        """Determine alert type based on liquidation side and amount."""
        side_lower = side.lower()

        if amount >= 100000:  # $100K+
            return f"huge_{side_lower}"
        elif amount >= 25000:  # $25K+
            return f"large_{side_lower}"
        elif amount >= 5000:  # $5K+
            return f"medium_{side_lower}"
        else:  # <$5K
            return f"small_{side_lower}"

    def play_sound(self, sound_type: str):
        """Play alert sound based on type."""
        if not self.enabled:
            return

        try:
            if self.system == 'darwin':  # macOS
                self._play_macos_sound(sound_type)
            elif self.system == 'linux':
                self._play_linux_sound(sound_type)
            elif self.system == 'windows':
                self._play_windows_sound(sound_type)
            else:
                self._play_fallback_sound(sound_type)
        except Exception as e:
            logger.warning(f"Could not play alert sound: {e}")

    def _play_macos_sound(self, sound_type: str):
        """Play sounds on macOS."""
        sound_map = {
            'low_tone': 'Ping',
            'medium_tone': 'Glass',
            'high_tone': 'Hero',
            'urgent_tone': 'Basso'
        }

        sound_name = sound_map.get(sound_type, 'Ping')

        # Use afplay to play system sounds
        try:
            subprocess.run(['afplay', f'/System/Library/Sounds/{sound_name}.aiff'],
                         capture_output=True, timeout=2)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            # Fallback to text-to-speech
            self._speak_alert(sound_type)

    def _play_linux_sound(self, sound_type: str):
        """Play sounds on Linux."""
        # Try different sound systems
        sound_commands = [
            ['paplay', f'/usr/share/sounds/freedesktop/stereo/{sound_type}.oga'],
            ['aplay', f'/usr/share/sounds/sound-icons/{sound_type}.wav'],
            ['beep', '-f', self._get_frequency(sound_type), '-l', '200']
        ]

        for cmd in sound_commands:
            try:
                subprocess.run(cmd, capture_output=True, timeout=1)
                return
            except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError):
                continue

        # Fallback to beep
        self._beep_alert(sound_type)

    def _play_windows_sound(self, sound_type: str):
        """Play sounds on Windows."""
        try:
            import winsound
            frequency = self._get_frequency(sound_type)
            duration = 300
            winsound.Beep(frequency, duration)
        except ImportError:
            self._beep_alert(sound_type)

    def _play_fallback_sound(self, sound_type: str):
        """Fallback sound method using print statements."""
        # Create terminal bell sounds using different patterns
        if sound_type == 'urgent_tone':
            print('\a\a\a\a', end='', flush=True)  # Multiple bells
        elif sound_type == 'high_tone':
            print('\a\a', end='', flush=True)
        else:
            print('\a', end='', flush=True)

    def _get_frequency(self, sound_type: str) -> int:
        """Get frequency for different alert types."""
        frequency_map = {
            'low_tone': 400,
            'medium_tone': 600,
            'high_tone': 800,
            'urgent_tone': 1000
        }
        return frequency_map.get(sound_type, 600)

    def _speak_alert(self, sound_type: str):
        """Use text-to-speech for alerts."""
        try:
            message = f"Liquidation alert {sound_type}"
            subprocess.run(['say', message], capture_output=True, timeout=2)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

    def _beep_alert(self, sound_type: str):
        """Simple beep alert using terminal bell."""
        beep_patterns = {
            'low_tone': '\a',
            'medium_tone': '\a\a',
            'high_tone': '\a\a\a',
            'urgent_tone': '\a\a\a\a'
        }
        print(beep_patterns.get(sound_type, '\a'), end='', flush=True)

    def alert_liquidation(self, symbol: str, side: str, amount: float):
        """Trigger alert for liquidation event."""
        if not self.enabled:
            return

        alert_type = self.get_alert_type(side, amount)
        alert_config = self.alerts.get(alert_type, self.alerts['small_long'])

        # Play sound
        self.play_sound(alert_config['sound'])

        # Log alert
        message = f"🔔 {alert_config['message']}: {symbol} ${amount:,.0f}"
        colored_message = alert_config['color'](message)
        print(colored_message, flush=True)

        # For huge liquidations, add extra notification
        if 'huge' in alert_type:
            print(alert_config['color']("🚨🚨 MASSIVE LIQUIDATION DETECTED 🚨🚨"), flush=True)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/realtime_liquidation_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RealtimeLiquidationMonitor:
    """Real-time liquidation data monitor for cryptocurrency futures."""

    def __init__(self, symbols: List[str], output_dir: str = "data/realtime_liquidations",
                 duration_hours: Optional[int] = None, verbose: bool = False, alerts: bool = True):
        """
        Initialize the liquidation monitor.

        Args:
            symbols: List of cryptocurrency symbols to monitor (e.g., ['BTC', 'ETH'])
            output_dir: Directory to save liquidation data
            duration_hours: How long to run (None = indefinite)
        """
        self.symbols = [s.upper() for s in symbols]
        self.output_dir = Path(output_dir)
        self.duration_hours = duration_hours

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Data storage
        self.liquidation_data: Dict[str, List[Dict]] = {symbol: [] for symbol in self.symbols}
        self.last_saved_timestamps = {symbol: None for symbol in self.symbols}  # Track last saved data per symbol
        self.stats = {
            'start_time': datetime.now(),
            'total_liquidations': 0,
            'symbol_stats': {symbol: {'count': 0, 'volume': 0.0} for symbol in self.symbols},
            'last_update': datetime.now()
        }

        # Control flags
        self.running = True
        self.connected = False
        self.verbose = verbose

        # Alert system
        self.alert_system = AlertSystem(enabled=alerts)

        # WebSocket URLs
        self.ws_base_url = "wss://fstream.binance.com/ws/"

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Load existing data timestamps to prevent overwriting
        self.load_existing_data_timestamps()

        logger.info(Colors.success(f"Initialized liquidation monitor for symbols: {self.symbols}"))

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    async def connect_and_monitor(self):
        """Main connection and monitoring loop using separate connections per symbol."""
        logger.info("Starting real-time liquidation monitoring...")

        # Calculate end time if duration is specified
        end_time = None
        if self.duration_hours:
            end_time = datetime.now() + timedelta(hours=self.duration_hours)
            logger.info(f"Will run for {self.duration_hours} hours until {end_time}")

        # Start statistics display thread
        stats_thread = threading.Thread(target=self.display_stats_loop, daemon=True)
        stats_thread.start()

        try:
            # Create separate monitoring tasks for each symbol
            tasks = []
            for symbol in self.symbols:
                task = asyncio.create_task(self.monitor_symbol_liquidations(symbol, end_time))
                tasks.append(task)

            logger.info(f"Started monitoring {len(self.symbols)} symbols for liquidations")

            # Wait for all monitoring tasks
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error in liquidation monitoring: {e}")

        finally:
            # Final save of all data
            await self.save_data_to_files()
            self.print_final_summary()

    async def monitor_symbol_liquidations(self, symbol: str, end_time: datetime = None):
        """Monitor liquidations for a single symbol."""
        stream_name = f"{symbol.lower()}usdt@forceOrder"
        ws_url = f"{self.ws_base_url}{stream_name}"

        logger.info(f"Connecting to {symbol} liquidation stream: {ws_url}")

        retry_count = 0
        max_retries = 5

        while self.running and retry_count < max_retries:
            try:
                async with websockets.connect(ws_url) as ws:
                    logger.info(Colors.success(f"✅ Connected to {symbol} liquidation stream"))
                    self.connected = True

                    while self.running:
                        # Check duration limit
                        if end_time and datetime.now() >= end_time:
                            logger.info(f"Duration limit reached ({self.duration_hours} hours)")
                            return

                        try:
                            # Set a timeout for receiving messages
                            message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            await self.process_message(message, symbol)

                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            await ws.ping()
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

        logger.info(f"Stopped monitoring {symbol} liquidations")

    def load_existing_data_timestamps(self):
        """Load timestamps from existing CSV files to prevent overwriting data."""
        try:
            for symbol in self.symbols:
                csv_file = self.output_dir / symbol.lower() / f"{symbol.lower()}_liquidations_realtime.csv"
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
                                            logger.info(f"📅 Loaded existing liquidation data for {symbol} up to {self.last_saved_timestamps[symbol]}")
                                        except Exception as parse_error:
                                            logger.warning(f"Failed to parse timestamp '{timestamp_str}' for {symbol}: {parse_error}")
                                            # Try alternative parsing
                                            try:
                                                # Handle different timestamp formats
                                                if 'T' in timestamp_str:
                                                    self.last_saved_timestamps[symbol] = pd.to_datetime(timestamp_str, format='%Y-%m-%dT%H:%M:%S.%f')
                                                else:
                                                    self.last_saved_timestamps[symbol] = pd.to_datetime(timestamp_str, format='%Y-%m-%d %H:%M:%S.%f')
                                                logger.info(f"📅 Loaded existing liquidation data for {symbol} up to {self.last_saved_timestamps[symbol]} (alternative parsing)")
                                            except Exception as alt_error:
                                                logger.error(f"Failed alternative timestamp parsing for {symbol}: {alt_error}")
                                                self.last_saved_timestamps[symbol] = None
                    except Exception as e:
                        logger.warning(f"Could not parse existing liquidation data timestamp for {symbol}: {e}")
        except Exception as e:
            logger.error(f"Error loading existing liquidation data timestamps: {e}")

    async def process_message(self, message: str, symbol: str):
        """Process incoming WebSocket message for a specific symbol."""
        try:
            data = json.loads(message)

            # Handle Binance Futures force order events
            # The data comes directly in the message, not wrapped in 'stream'/'data'
            if data.get('e') == 'forceOrder':  # Force order event
                await self.process_liquidation_event(data, symbol)
            else:
                # Log stream activity based on verbose setting
                if self.verbose:
                    logger.info(f"📡 {Colors.data(symbol)} stream message: {Colors.data(data.get('e', 'unknown'))}")
                else:
                    # Just log that we're receiving data from each symbol periodically (non-verbose)
                    if hasattr(self, '_last_log_time'):
                        current_time = datetime.now()
                        if (current_time - self._last_log_time).seconds >= 300:  # Every 5 minutes
                            logger.info(f"📡 Active monitoring {len(self.symbols)} symbols - receiving live data streams")
                            self._last_log_time = current_time
                    else:
                        self._last_log_time = datetime.now()
                        logger.info(f"📡 Started monitoring {len(self.symbols)} symbols - live data streams active")

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse {symbol} WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing {symbol} message: {e}")

    async def process_liquidation_event(self, event_data: Dict, symbol: str):
        """Process a liquidation event and store the data."""
        try:
            # Extract liquidation details from Binance Futures forceOrder event
            # The order details are in the 'o' field
            order_data = event_data.get('o', {})

            # Map side: BUY = LONG liquidation, SELL = SHORT liquidation
            side = order_data.get('S', '')
            if side == 'BUY':
                liquidation_side = 'LONG'
            elif side == 'SELL':
                liquidation_side = 'SHORT'
            else:
                liquidation_side = side

            # Extract price and quantity
            price = float(order_data.get('ap', 0))  # Average price
            quantity = float(order_data.get('q', 0))  # Original quantity
            amount = price * quantity if price and quantity else 0

            liquidation = {
                'timestamp': datetime.fromtimestamp(event_data['E'] / 1000),  # Event time
                'symbol': symbol,
                'side': liquidation_side,
                'price': price,
                'quantity': quantity,
                'amount': amount,
                'event_time': event_data['E'],  # Raw timestamp
                'event_type': 'liquidation'
            }

            # Add to data storage
            self.liquidation_data[symbol].append(liquidation)

            # Update statistics
            self.stats['total_liquidations'] += 1
            self.stats['symbol_stats'][symbol]['count'] += 1
            self.stats['symbol_stats'][symbol]['volume'] += liquidation['amount']
            self.stats['last_update'] = datetime.now()

            # Trigger alert system
            self.alert_system.alert_liquidation(symbol, liquidation['side'], liquidation['amount'])

            # Log significant liquidations
            if liquidation['amount'] > 10000:  # $10K+ liquidations
                amount_str = f"${liquidation['amount']:,.0f}"
                quantity_str = f"{liquidation['quantity']:.2f}"
                price_str = f"${liquidation['price']:.2f}"
                logger.info(f"💰 LARGE LIQUIDATION: {Colors.data(symbol)} {Colors.data(amount_str)} {Colors.warning(liquidation['side'])} {Colors.data(quantity_str)} @ {Colors.data(price_str)}")

            # Periodic save to file (every 100 liquidations or 5 minutes)
            total_liquidations = sum(len(data) for data in self.liquidation_data.values())
            if total_liquidations % 100 == 0:
                await self.save_data_to_files()

        except Exception as e:
            logger.error(f"Error processing liquidation event: {e}")

    async def save_data_to_files(self):
        """Save new liquidation data to CSV files (append mode) and create compressed backups."""
        try:
            for symbol in self.symbols:
                if not self.liquidation_data[symbol]:
                    continue

                # Create symbol directory
                symbol_dir = self.output_dir / symbol.lower()
                symbol_dir.mkdir(exist_ok=True)

                # Save to CSV (append mode)
                csv_file = symbol_dir / f"{symbol.lower()}_liquidations_realtime.csv"

                # Filter data that's newer than the last saved timestamp
                last_saved = self.last_saved_timestamps[symbol]
                if last_saved:
                    # Only save data newer than what was last saved
                    original_count = len(self.liquidation_data[symbol])
                    new_data = [entry for entry in self.liquidation_data[symbol]
                              if entry['timestamp'] > last_saved]
                    logger.debug(f"💧 {symbol}: {original_count} total records, {len(new_data)} new records since {last_saved}")
                else:
                    # First time saving, save all data
                    new_data = self.liquidation_data[symbol]
                    logger.info(f"💧 {symbol}: First time saving {len(new_data)} records")

                if new_data:
                    # Convert new data to DataFrame
                    df = pd.DataFrame(new_data)

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

                    logger.info(f"💾 Appended liquidation data: {csv_file} ({len(new_data)} new events)")

                    # Create compressed backup every hour
                    now = datetime.now()
                    if now.minute == 0:  # Top of the hour
                        compressed_file = symbol_dir / f"{symbol.lower()}_liquidations_{now.strftime('%Y%m%d_%H')}.csv.gz"
                        # For compressed files, we save all current data (not just new)
                        all_df = pd.DataFrame(self.liquidation_data[symbol])
                        with gzip.open(compressed_file, 'wt') as f:
                            all_df.to_csv(f, index=False)
                        logger.info(f"📦 Saved compressed liquidation data: {compressed_file}")

        except Exception as e:
            logger.error(f"Error saving liquidation data: {e}")

    async def save_compressed_data(self, symbol: str, df: pd.DataFrame):
        """Save compressed version of the data."""
        try:
            symbol_dir = self.output_dir / symbol.lower()
            timestamp = datetime.now().strftime('%Y%m%d_%H')
            compressed_file = symbol_dir / f"{symbol.lower()}_liquidations_{timestamp}.csv.gz"

            with gzip.open(compressed_file, 'wt', encoding='utf-8') as f:
                df.to_csv(f, index=False)

            logger.info(f"📦 Saved compressed data: {compressed_file}")

        except Exception as e:
            logger.error(f"Error saving compressed data: {e}")

    def display_stats_loop(self):
        """Display real-time statistics every 30 seconds with colored output."""
        while self.running:
            time.sleep(30)  # Update every 30 seconds

            if not self.liquidation_data:
                continue

            # Clear screen and show stats with colors
            print("\n" + Colors.header("="*70))
            print(Colors.header("📊 REAL-TIME LIQUIDATION MONITOR"))
            print(Colors.header("="*70))
            print(f"⏰ Running since: {Colors.data(self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S'))}")
            connection_status = Colors.success("✅ Connected") if self.connected else Colors.error("❌ Disconnected")
            print(f"🔗 Connection: {connection_status}")
            total_liq_formatted = f"{self.stats['total_liquidations']:,}"
            print(f"💰 Total Liquidations: {Colors.highlight(total_liq_formatted)}")

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
            print(Colors.dim("-" * 40))
            for symbol in self.symbols:
                stats = self.stats['symbol_stats'][symbol]
                recent_count = len([l for l in self.liquidation_data[symbol]
                                  if (datetime.now() - l['timestamp']).seconds < 1800])  # Last 30 min

                # Color coding based on activity
                if stats['count'] > 100:
                    count_display = Colors.success(f"{stats['count']:6d}")
                elif stats['count'] > 10:
                    count_display = Colors.data(f"{stats['count']:6d}")
                else:
                    count_display = Colors.warning(f"{stats['count']:6d}")

                volume_display = Colors.data(f"${stats['volume']:,.0f}")

                if recent_count > 5:
                    recent_display = Colors.success(f"{recent_count}")
                elif recent_count > 0:
                    recent_display = Colors.warning(f"{recent_count}")
                else:
                    recent_display = Colors.dim(f"{recent_count}")

                print(f"  {Colors.info(symbol):6} {count_display} {volume_display} Recent (30min): {recent_display}")

            print(f"\n📁 Data saved to: {Colors.data(str(self.output_dir))}")
            print(Colors.info("💡 Press Ctrl+C to stop monitoring"))
            print(Colors.header("="*70))

    async def run_monitor(self):
        """Run the liquidation monitor."""
        try:
            while self.running:
                try:
                    await self.connect_and_monitor()

                    # If we get here, connection was lost - wait before reconnecting
                    if self.running:
                        logger.info("Attempting reconnection in 5 seconds...")
                        await asyncio.sleep(5)

                except Exception as e:
                    logger.error(f"Connection error: {e}")
                    if self.running:
                        logger.info("Retrying connection in 10 seconds...")
                        await asyncio.sleep(10)

        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        finally:
            # Final save of all data
            await self.save_data_to_files()
            self.print_final_summary()

    def print_final_summary(self):
        """Print final summary when monitoring stops with colored output."""
        print("\n" + Colors.header("="*80))
        print(Colors.success("🏁 LIQUIDATION MONITORING SESSION COMPLETE"))
        print(Colors.header("="*80))

        runtime = datetime.now() - self.stats['start_time']
        runtime_str = str(runtime).split('.')[0]
        print(f"⏰ Session Duration: {Colors.data(runtime_str)}")

        total_liq = self.stats['total_liquidations']
        total_liq_highlight = f"{total_liq:,}"
        print(f"💰 Total Liquidations Captured: {Colors.highlight(total_liq_highlight)}")

        if total_liq > 0:
            avg_per_hour = total_liq / (runtime.total_seconds() / 3600)
            print(f"📊 Average Liquidations/Hour: {Colors.data(f'{avg_per_hour:.1f}')}")

        print(f"\n{Colors.header('📈 Final Symbol Statistics:')}")
        print(Colors.dim("-" * 50))
        for symbol in self.symbols:
            stats = self.stats['symbol_stats'][symbol]
            if stats['count'] > 0:
                avg_size = stats['volume'] / stats['count']
                count_display = Colors.success(f"{stats['count']:6d}") if stats['count'] > 50 else Colors.data(f"{stats['count']:6d}")
                volume_display = Colors.data(f"${stats['volume']:,.0f}")
                avg_display = Colors.data(f"${avg_size:.0f}")
                print(f"  {Colors.info(symbol):6} {count_display} Volume: {volume_display} Avg: {avg_display}")
            else:
                print(f"  {Colors.warning(symbol):6} {Colors.dim('No liquidations captured')}")

        print(f"\n📁 Data Location: {Colors.data(str(self.output_dir))}")
        files_found = 0
        for symbol in self.symbols:
            csv_file = self.output_dir / symbol.lower() / f"{symbol.lower()}_liquidations_realtime.csv"
            if csv_file.exists():
                size_mb = csv_file.stat().st_size / (1024 * 1024)
                size_display = Colors.success(f"{size_mb:.1f} MB") if size_mb > 1 else Colors.data(f"{size_mb:.1f} MB")
                print(f"   {Colors.info(symbol):6}: {size_display}")
                files_found += 1

        if files_found == 0:
            print(Colors.warning("   No data files were created"))

        print(f"\n{Colors.success('🎯 Next Steps:')}")
        print("  • Analyze liquidation patterns for market signals")
        print("  • Use data for capitulation detection algorithms")
        print("  • Monitor liquidation clusters for trading opportunities")
        print(Colors.header("="*80))


def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Real-Time Cryptocurrency Liquidation Monitor')
    parser.add_argument('--symbols', nargs='+', help='Cryptocurrency symbols to monitor (e.g., BTC ETH)')
    parser.add_argument('--all', action='store_true', help='Monitor all major cryptocurrencies')
    parser.add_argument('--duration', type=int, help='Monitoring duration in hours (default: indefinite)')
    parser.add_argument('--output', default='data/realtime_liquidations',
                       help='Output directory for liquidation data')
    parser.add_argument('--quality-data', action='store_true',
                       help='Monitor symbols from quality data categories (full_history, good_coverage, established)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show verbose logging including all WebSocket messages')
    parser.add_argument('--no-alerts', action='store_true',
                       help='Disable audio/visual alerts for liquidations')

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
        print("  python realtime_liquidation_monitor.py --symbols BTC ETH")
        print("  python realtime_liquidation_monitor.py --all")
        print("  python realtime_liquidation_monitor.py --quality-data")
        print("  python realtime_liquidation_monitor.py --symbols BTC --duration 24")
        sys.exit(1)

    # Validate symbols (extended list for quality data categories)
    valid_symbols = ['AAVE', 'ADA', 'ALGO', 'AVAX', 'BCH', 'BNB', 'BTC', 'DOGE', 'DOT', 'ETC', 'ETH',
                    'FIL', 'HBAR', 'ICP', 'LINK', 'LTC', 'SHIB', 'SOL', 'THETA', 'TRX', 'UNI',
                    'USDC', 'VET', 'XLM', 'XMR', 'XRP', 'ZEC']

    invalid_symbols = [s for s in symbols if s.upper() not in valid_symbols]
    if invalid_symbols:
        print(f"⚠️  Warning: These symbols may not be available on Binance Futures: {invalid_symbols}")

    # Create and run monitor
    monitor = RealtimeLiquidationMonitor(
        symbols=symbols,
        output_dir=args.output,
        duration_hours=args.duration,
        verbose=args.verbose,
        alerts=not args.no_alerts
    )

    print(Colors.success("🚀 Starting Real-Time Liquidation Monitor"))
    print(f"📊 Monitoring: {Colors.data(', '.join(symbols))}")
    print(f"📁 Output: {Colors.data(args.output)}")
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
