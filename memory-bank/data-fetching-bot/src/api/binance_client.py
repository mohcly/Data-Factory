"""
Binance API Client for Data Fetching Bot
Handles Binance-specific API integration with authentication and rate limiting
"""

import os
import time
import hmac
import hashlib
import asyncio
import requests
import logging
from typing import Dict, List, Optional
from urllib.parse import urlencode
from datetime import datetime

from .base_api_client import BaseAPIClient

logger = logging.getLogger(__name__)


class BinanceAPIClient(BaseAPIClient):
    """Binance API client with authentication and historical data fetching"""

    def __init__(self, api_key: str = None, api_secret: str = None):
        """Initialize Binance API client"""
        super().__init__(
            name="Binance",
            rate_limit=1200,  # 1200 requests per minute for authenticated users
            base_url="https://api.binance.com"
        )

        self.api_key = api_key or os.getenv('BINANCE_API_KEY')
        self.api_secret = api_secret or os.getenv('BINANCE_API_SECRET')

        # Testnet URLs for development
        self.use_testnet = os.getenv('BINANCE_TESTNET', 'false').lower() == 'true'
        if self.use_testnet:
            self.base_url = "https://testnet.binance.vision"

        logger.info(f"Initialized Binance API client (testnet: {self.use_testnet})")

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Binance API requests"""
        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        if self.use_testnet:
            headers['Content-Type'] = 'application/json'

        return headers

    def _sign_request(self, params: Dict) -> Dict:
        """Sign request parameters for authenticated endpoints"""
        if not self.api_secret:
            return params

        # Add timestamp
        params['timestamp'] = int(time.time() * 1000)

        # Create query string
        query_string = urlencode(params)

        # Create signature
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        params['signature'] = signature
        return params

    async def _make_api_call(self, url: str, params: Dict = None,
                           method: str = 'GET', headers: Dict = None) -> Dict:
        """Make actual HTTP request to Binance API"""
        if params is None:
            params = {}

        # Prepare headers
        if headers is None:
            headers = self._get_headers()

        # Sign request if needed (for authenticated endpoints)
        if method == 'GET' and any(key in url for key in ['account', 'order', 'trading']):
            params = self._sign_request(params)

        # Build final URL
        if method == 'GET' and params:
            url += '?' + urlencode(params)

        # Make the request
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=params if method != 'GET' else None,
            timeout=30
        )

        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            logger.warning(f"Binance rate limit exceeded. Waiting {retry_after} seconds")
            time.sleep(retry_after)
            # Retry the request
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=params if method != 'GET' else None,
                timeout=30
            )

        response.raise_for_status()
        return response.json()

    async def get_klines(self, symbol: str, interval: str = '1h',
                        start_time: str = None, end_time: str = None,
                        limit: int = 500) -> List[Dict]:
        """Fetch historical klines (candlestick) data"""
        params = {
            'symbol': symbol.upper(),
            'interval': interval,
            'limit': min(limit, 1000)  # Binance limit is 1000
        }

        if start_time:
            params['startTime'] = self._parse_timestamp(start_time)
        if end_time:
            params['endTime'] = self._parse_timestamp(end_time)

        try:
            response = await self.make_request('/api/v3/klines', params=params)

            # Transform response to our standard format
            klines = []
            for kline in response:
                klines.append({
                    'timestamp': datetime.fromtimestamp(kline[0] / 1000).isoformat(),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5]),
                    'close_time': kline[6],
                    'quote_asset_volume': float(kline[7]),
                    'number_of_trades': kline[8],
                    'taker_buy_base_asset_volume': float(kline[9]),
                    'taker_buy_quote_asset_volume': float(kline[10])
                })

            return klines

        except Exception as e:
            logger.error(f"Failed to fetch klines for {symbol}: {e}")
            raise

    async def get_ticker_price(self, symbol: str) -> Dict:
        """Get current ticker price for a symbol"""
        params = {'symbol': symbol.upper()}

        try:
            response = await self.make_request('/api/v3/ticker/price', params=params)
            return {
                'symbol': response['symbol'],
                'price': float(response['price']),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to fetch ticker price for {symbol}: {e}")
            raise

    async def get_24hr_ticker_stats(self, symbol: str) -> Dict:
        """Get 24hr ticker statistics for a symbol"""
        params = {'symbol': symbol.upper()}

        try:
            response = await self.make_request('/api/v3/ticker/24hr', params=params)

            return {
                'symbol': response['symbol'],
                'price_change': float(response['priceChange']),
                'price_change_percent': float(response['priceChangePercent']),
                'weighted_avg_price': float(response['weightedAvgPrice']),
                'prev_close_price': float(response['prevClosePrice']),
                'last_price': float(response['lastPrice']),
                'bid_price': float(response['bidPrice']),
                'ask_price': float(response['askPrice']),
                'open_price': float(response['openPrice']),
                'high_price': float(response['highPrice']),
                'low_price': float(response['lowPrice']),
                'volume': float(response['volume']),
                'open_time': datetime.fromtimestamp(response['openTime'] / 1000).isoformat(),
                'close_time': datetime.fromtimestamp(response['closeTime'] / 1000).isoformat(),
                'count': int(response['count'])
            }
        except Exception as e:
            logger.error(f"Failed to fetch 24hr stats for {symbol}: {e}")
            raise

    async def get_symbol_info(self, symbol: str = None) -> List[Dict]:
        """Get exchange information for symbols"""
        params = {}
        if symbol:
            params['symbol'] = symbol.upper()

        try:
            response = await self.make_request('/api/v3/exchangeInfo', params=params)

            if symbol:
                # Return info for specific symbol
                for symbol_info in response['symbols']:
                    if symbol_info['symbol'] == symbol.upper():
                        return symbol_info
                return None
            else:
                # Return all symbols
                return response['symbols']

        except Exception as e:
            logger.error(f"Failed to fetch symbol info: {e}")
            raise

    async def ping(self) -> bool:
        """Test connectivity to Binance API"""
        try:
            response = await self.make_request('/api/v3/ping')
            return True
        except Exception as e:
            logger.error(f"Binance API ping failed: {e}")
            return False

    def _parse_timestamp(self, timestamp_str: str) -> int:
        """Parse timestamp string to milliseconds since epoch"""
        if isinstance(timestamp_str, str):
            # Try to parse ISO format first
            try:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            except:
                pass

            # Try to parse as Unix timestamp
            try:
                return int(float(timestamp_str) * 1000)
            except:
                pass

        # Return as-is if already a number
        return int(timestamp_str)

    async def get_historical_klines_batch(self, symbol: str, interval: str = '1h',
                                        start_date: str = None, end_date: str = None,
                                        max_requests: int = 100) -> List[Dict]:
        """Fetch historical klines in batches to avoid API limits"""
        all_klines = []
        current_start = start_date

        request_count = 0
        while request_count < max_requests:
            try:
                # Calculate end time for this batch
                if not current_start:
                    break

                # For large date ranges, fetch in smaller chunks
                batch_klines = await self.get_klines(
                    symbol=symbol,
                    interval=interval,
                    start_time=current_start,
                    limit=1000
                )

                if not batch_klines:
                    break

                all_klines.extend(batch_klines)
                request_count += 1

                # Update start time for next batch
                last_kline = batch_klines[-1]
                current_start = datetime.fromisoformat(last_kline['timestamp']).isoformat()

                # If we've reached the end date, break
                if end_date and current_start >= end_date:
                    break

                # Add small delay to avoid overwhelming the API
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in batch fetching for {symbol}: {e}")
                break

        logger.info(f"Fetched {len(all_klines)} klines for {symbol} in {request_count} requests")
        return all_klines