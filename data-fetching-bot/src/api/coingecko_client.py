"""
CoinGecko API Client for Data Fetching Bot
Handles CoinGecko free tier API integration with rate limiting
"""

import os
import asyncio
import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from .base_api_client import BaseAPIClient

logger = logging.getLogger(__name__)


class CoinGeckoAPIClient(BaseAPIClient):
    """CoinGecko API client for free tier access"""

    def __init__(self):
        """Initialize CoinGecko API client"""
        super().__init__(
            name="CoinGecko",
            rate_limit=30,  # 10-50 requests per minute for free tier
            base_url="https://api.coingecko.com/api/v3"
        )

        # CoinGecko specific rate limiting (more conservative)
        self.demo_plan_limit = 30  # requests per minute for demo plan

        logger.info("Initialized CoinGecko API client")

    async def _make_api_call(self, url: str, params: Dict = None,
                           method: str = 'GET', headers: Dict = None) -> Dict:
        """Make actual HTTP request to CoinGecko API"""
        if params is None:
            params = {}

        # Prepare headers
        if headers is None:
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'Data-Fetching-Bot/1.0.0'
            }

        # Build final URL
        if method == 'GET' and params:
            url += '?' + '&'.join([f"{k}={v}" for k, v in params.items()])

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
            retry_after = int(response.headers.get('Retry-After', 60))
            logger.warning(f"CoinGecko rate limit exceeded. Waiting {retry_after} seconds")
            await asyncio.sleep(retry_after)
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

    async def get_coins_list(self) -> List[Dict]:
        """Get list of all supported coins"""
        try:
            response = await self.make_request('/coins/list')

            # Transform to include timestamp
            coins = []
            for coin in response:
                coins.append({
                    'id': coin.get('id'),
                    'symbol': coin.get('symbol'),
                    'name': coin.get('name'),
                    'timestamp': datetime.now().isoformat()
                })

            return coins

        except Exception as e:
            logger.error(f"Failed to fetch coins list: {e}")
            raise

    async def get_coin_history(self, coin_id: str, date: str) -> Dict:
        """Get historical data for a coin on a specific date"""
        params = {
            'date': date,
            'localization': 'false'
        }

        try:
            response = await self.make_request(f'/coins/{coin_id}/history', params=params)

            # Transform response to our standard format
            market_data = response.get('market_data', {})

            return {
                'coin_id': coin_id,
                'date': date,
                'current_price': market_data.get('current_price', {}).get('usd'),
                'market_cap': market_data.get('market_cap', {}).get('usd'),
                'total_volume': market_data.get('total_volume', {}).get('usd'),
                'high_24h': market_data.get('high_24h', {}).get('usd'),
                'low_24h': market_data.get('low_24h', {}).get('usd'),
                'price_change_24h': market_data.get('price_change_24h'),
                'price_change_percentage_24h': market_data.get('price_change_percentage_24h'),
                'market_cap_change_24h': market_data.get('market_cap_change_24h'),
                'market_cap_change_percentage_24h': market_data.get('market_cap_change_percentage_24h'),
                'circulating_supply': market_data.get('circulating_supply'),
                'total_supply': market_data.get('total_supply'),
                'max_supply': market_data.get('max_supply'),
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to fetch coin history for {coin_id} on {date}: {e}")
            raise

    async def get_coin_market_chart(self, coin_id: str, vs_currency: str = 'usd',
                                  days: str = 'max', interval: str = 'daily') -> Dict:
        """Get historical market data for a coin"""
        params = {
            'vs_currency': vs_currency,
            'days': days,
            'interval': interval
        }

        try:
            response = await self.make_request(f'/coins/{coin_id}/market_chart', params=params)

            # Transform response to OHLCV format
            prices = response.get('prices', [])
            market_caps = response.get('market_caps', [])
            total_volumes = response.get('total_volumes', [])

            # Combine data points
            data_points = []
            for i, price_point in enumerate(prices):
                timestamp = datetime.fromtimestamp(price_point[0] / 1000)

                # Find corresponding market cap and volume
                market_cap = 0
                volume = 0

                if i < len(market_caps):
                    market_cap = market_caps[i][1]

                if i < len(total_volumes):
                    volume = total_volumes[i][1]

                data_points.append({
                    'timestamp': timestamp.isoformat(),
                    'price': price_point[1],
                    'market_cap': market_cap,
                    'total_volume': volume
                })

            return {
                'coin_id': coin_id,
                'vs_currency': vs_currency,
                'data_points': data_points,
                'total_points': len(data_points)
            }

        except Exception as e:
            logger.error(f"Failed to fetch market chart for {coin_id}: {e}")
            raise

    async def get_simple_price(self, coin_ids: List[str], vs_currencies: List[str] = None,
                             include_market_cap: bool = False, include_24hr_vol: bool = False,
                             include_24hr_change: bool = False, include_last_updated_at: bool = False) -> Dict:
        """Get current price for multiple coins"""
        if vs_currencies is None:
            vs_currencies = ['usd']

        params = {
            'ids': ','.join(coin_ids),
            'vs_currencies': ','.join(vs_currencies)
        }

        if include_market_cap:
            params['include_market_cap'] = 'true'
        if include_24hr_vol:
            params['include_24hr_vol'] = 'true'
        if include_24hr_change:
            params['include_24hr_change'] = 'true'
        if include_last_updated_at:
            params['include_last_updated_at'] = 'true'

        try:
            response = await self.make_request('/simple/price', params=params)

            # Add timestamp to response
            result = {}
            for coin_id, data in response.items():
                result[coin_id] = {
                    **data,
                    'timestamp': datetime.now().isoformat()
                }

            return result

        except Exception as e:
            logger.error(f"Failed to fetch simple prices for {coin_ids}: {e}")
            raise

    async def get_coins_markets(self, vs_currency: str = 'usd', category: str = None,
                              order: str = 'market_cap_desc', per_page: int = 100,
                              page: int = 1, sparkline: bool = False) -> List[Dict]:
        """Get market data for coins by market cap"""
        params = {
            'vs_currency': vs_currency,
            'order': order,
            'per_page': min(per_page, 250),  # CoinGecko limit
            'page': page,
            'sparkline': 'true' if sparkline else 'false'
        }

        if category:
            params['category'] = category

        try:
            response = await self.make_request('/coins/markets', params=params)

            # Transform and add timestamp
            markets = []
            for coin in response:
                markets.append({
                    'id': coin.get('id'),
                    'symbol': coin.get('symbol'),
                    'name': coin.get('name'),
                    'image': coin.get('image'),
                    'current_price': coin.get('current_price'),
                    'market_cap': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'fully_diluted_valuation': coin.get('fully_diluted_valuation'),
                    'total_volume': coin.get('total_volume'),
                    'high_24h': coin.get('high_24h'),
                    'low_24h': coin.get('low_24h'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'market_cap_change_24h': coin.get('market_cap_change_24h'),
                    'market_cap_change_percentage_24h': coin.get('market_cap_change_percentage_24h'),
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    'ath': coin.get('ath'),
                    'ath_change_percentage': coin.get('ath_change_percentage'),
                    'ath_date': coin.get('ath_date'),
                    'atl': coin.get('atl'),
                    'atl_change_percentage': coin.get('atl_change_percentage'),
                    'atl_date': coin.get('atl_date'),
                    'last_updated': coin.get('last_updated'),
                    'timestamp': datetime.now().isoformat()
                })

            return markets

        except Exception as e:
            logger.error(f"Failed to fetch coins markets: {e}")
            raise

    async def ping(self) -> bool:
        """Test connectivity to CoinGecko API"""
        try:
            response = await self.make_request('/ping')
            return response.get('gecko_says') == "(V3) To the Moon!"
        except Exception as e:
            logger.error(f"CoinGecko API ping failed: {e}")
            return False

    async def get_supported_currencies(self) -> List[str]:
        """Get list of supported vs currencies"""
        try:
            response = await self.make_request('/simple/supported_vs_currencies')
            return response
        except Exception as e:
            logger.error(f"Failed to fetch supported currencies: {e}")
            raise