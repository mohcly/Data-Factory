"""
Base API Client for Data Fetching Bot
Provides foundation for API integrations with rate limiting and health monitoring
"""

import asyncio
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseAPIClient(ABC):
    """Abstract base class for API clients with rate limiting and health monitoring"""

    def __init__(self, name: str, rate_limit: int, base_url: str = None):
        """Initialize API client with rate limiting configuration"""
        self.name = name
        self.rate_limit = rate_limit  # requests per minute
        self.base_url = base_url
        self.request_count = 0
        self.last_request_time = None
        self.health_status = "healthy"
        self.error_count = 0
        self.success_count = 0
        self.total_response_time = 0

        # Rate limiting state
        self._request_timestamps = []
        self._lock = asyncio.Lock()

    async def _enforce_rate_limit(self):
        """Enforce rate limiting to avoid API bans"""
        current_time = time.time()

        async with self._lock:
            # Remove timestamps older than 1 minute
            cutoff_time = current_time - 60
            self._request_timestamps = [ts for ts in self._request_timestamps if ts > cutoff_time]

            # Check if we're exceeding rate limit
            if len(self._request_timestamps) >= self.rate_limit:
                # Calculate wait time
                oldest_request = min(self._request_timestamps)
                wait_time = 60 - (current_time - oldest_request)

                if wait_time > 0:
                    logger.warning(f"Rate limit reached for {self.name}. Waiting {wait_time:.1f} seconds")
                    await asyncio.sleep(wait_time)

            # Add current timestamp
            self._request_timestamps.append(current_time)

    async def make_request(self, endpoint: str, params: Dict = None,
                          method: str = 'GET', headers: Dict = None) -> Dict:
        """Make rate-limited API request with error handling"""
        await self._enforce_rate_limit()

        start_time = time.time()
        self.request_count += 1
        self.last_request_time = datetime.now()

        try:
            # Prepare request
            url = f"{self.base_url}/{endpoint}" if self.base_url else endpoint
            if not headers:
                headers = {}

            # Add common headers
            headers.update({
                'User-Agent': 'Data-Fetching-Bot/1.0.0',
                'Accept': 'application/json'
            })

            # Make the actual API call (implemented by subclasses)
            response = await self._make_api_call(url, params, method, headers)

            # Calculate response time
            response_time = time.time() - start_time
            self.total_response_time += response_time

            # Update success metrics
            self.success_count += 1
            self._update_health_status()

            logger.debug(f"✅ {self.name} API call successful: {endpoint} ({response_time:.2f}s)")
            return response

        except Exception as e:
            # Update error metrics
            self.error_count += 1
            response_time = time.time() - start_time
            self.total_response_time += response_time

            self._update_health_status()
            logger.error(f"❌ {self.name} API call failed: {endpoint} - {str(e)}")

            raise e

    @abstractmethod
    async def _make_api_call(self, url: str, params: Dict, method: str, headers: Dict) -> Dict:
        """Abstract method to be implemented by subclasses"""
        pass

    def _update_health_status(self):
        """Update health status based on recent performance"""
        if self.error_count == 0:
            self.health_status = "healthy"
        elif self.error_count < 5:
            self.health_status = "degraded"
        else:
            self.health_status = "unhealthy"

    def is_healthy(self) -> bool:
        """Check if API is in healthy state"""
        return self.health_status == "healthy"

    def get_performance_metrics(self) -> Dict:
        """Get current performance metrics"""
        avg_response_time = (self.total_response_time / self.request_count) if self.request_count > 0 else 0
        success_rate = (self.success_count / self.request_count) if self.request_count > 0 else 0

        return {
            'api_name': self.name,
            'request_count': self.request_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate': success_rate,
            'average_response_time': avg_response_time,
            'health_status': self.health_status,
            'last_request_time': self.last_request_time.isoformat() if self.last_request_time else None
        }

    async def wait_for_health(self, max_wait_time: int = 300):
        """Wait for API to become healthy again"""
        start_time = time.time()

        while not self.is_healthy() and (time.time() - start_time) < max_wait_time:
            logger.info(f"Waiting for {self.name} API to recover...")
            await asyncio.sleep(30)  # Wait 30 seconds before checking again

        if not self.is_healthy():
            raise Exception(f"{self.name} API failed to recover within {max_wait_time} seconds")

    def reset_metrics(self):
        """Reset performance metrics (for testing or manual intervention)"""
        self.request_count = 0
        self.error_count = 0
        self.success_count = 0
        self.total_response_time = 0
        self.health_status = "healthy"
        logger.info(f"Reset metrics for {self.name} API client")