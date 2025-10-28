"""
API Manager for Data Fetching Bot
Handles API failover, load balancing, and intelligent request routing
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import random

logger = logging.getLogger(__name__)


class APIManager:
    """Manages multiple API clients with failover and load balancing"""

    def __init__(self, api_clients: List):
        """Initialize API manager with available clients"""
        self.api_clients = api_clients
        self.client_performance = {}
        self.active_client = None
        self.failover_history = []

        # Initialize performance tracking
        for client in self.api_clients:
            self.client_performance[client.name] = {
                'requests': 0,
                'successes': 0,
                'failures': 0,
                'avg_response_time': 0,
                'last_failure': None,
                'consecutive_failures': 0,
                'circuit_breaker_trips': 0,
                'last_circuit_reset': datetime.now()
            }

        logger.info("API Manager initialized with failover and load balancing")

    def _calculate_client_score(self, client_name: str) -> float:
        """Calculate a score for API client based on performance"""
        if client_name not in self.client_performance:
            return 0.0

        perf = self.client_performance[client_name]
        total_requests = perf['requests']

        if total_requests == 0:
            return 1.0  # New clients get highest priority

        success_rate = perf['successes'] / total_requests
        avg_response = perf['avg_response_time']

        # Score based on success rate (70% weight) and response time (30% weight)
        # Lower response time is better, so we invert it
        max_response_time = 10.0  # Assume 10 seconds max
        response_score = max(0, (max_response_time - avg_response) / max_response_time)

        # Penalize recent failures
        failure_penalty = min(0.5, perf['consecutive_failures'] * 0.1)

        return (success_rate * 0.7 + response_score * 0.3) - failure_penalty

    def _select_best_client(self, symbol: str = None) -> Optional[Any]:
        """Select the best available API client based on performance"""
        healthy_clients = []

        for client in self.api_clients:
            if client.is_healthy():
                score = self._calculate_client_score(client.name)
                healthy_clients.append((client, score))
            else:
                logger.debug(f"Client {client.name} is unhealthy, skipping")

        if not healthy_clients:
            logger.warning("No healthy API clients available")
            return None

        # Sort by score (highest first)
        healthy_clients.sort(key=lambda x: x[1], reverse=True)
        best_client, best_score = healthy_clients[0]

        logger.info(f"Selected {best_client.name} for request (score: {best_score".2f"})")
        return best_client

    def _update_performance_metrics(self, client_name: str, success: bool,
                                   response_time: float = 0.0):
        """Update performance metrics for an API client"""
        if client_name not in self.client_performance:
            return

        perf = self.client_performance[client_name]
        perf['requests'] += 1

        if success:
            perf['successes'] += 1
            perf['consecutive_failures'] = 0
            perf['last_success'] = datetime.now()

            # Update average response time
            if perf['requests'] == 1:
                perf['avg_response_time'] = response_time
            else:
                # Exponential moving average
                alpha = 0.1
                perf['avg_response_time'] = (alpha * response_time +
                                           (1 - alpha) * perf['avg_response_time'])
        else:
            perf['failures'] += 1
            perf['consecutive_failures'] += 1
            perf['last_failure'] = datetime.now()

            # Check for circuit breaker
            if perf['consecutive_failures'] >= 5:
                logger.warning(f"Circuit breaker tripped for {client_name}")
                perf['circuit_breaker_trips'] += 1

    async def make_request(self, operation: str, **kwargs) -> Dict:
        """Make a request with automatic failover and load balancing"""
        last_error = None
        attempted_clients = set()

        # Try up to 3 times with different clients
        for attempt in range(3):
            client = self._select_best_client(kwargs.get('symbol'))

            if not client:
                error_msg = "No healthy API clients available"
                logger.error(error_msg)
                raise Exception(error_msg)

            if client.name in attempted_clients:
                continue

            attempted_clients.add(client.name)

            try:
                logger.info(f"Attempting {operation} with {client.name} (attempt {attempt + 1}/3)")

                start_time = datetime.now()

                # Call the appropriate method based on operation
                if operation == 'get_klines':
                    result = await client.get_klines(**kwargs)
                elif operation == 'get_historical_klines_batch':
                    result = await client.get_historical_klines_batch(**kwargs)
                elif operation == 'get_coin_market_chart':
                    result = await client.get_coin_market_chart(**kwargs)
                else:
                    raise ValueError(f"Unknown operation: {operation}")

                response_time = (datetime.now() - start_time).total_seconds()

                # Update performance metrics
                self._update_performance_metrics(client.name, True, response_time)

                logger.info(f"✅ {operation} successful with {client.name} in {response_time:.".2f"")

                return result

            except Exception as e:
                response_time = (datetime.now() - start_time).total_seconds()
                last_error = e

                # Update performance metrics
                self._update_performance_metrics(client.name, False, response_time)

                logger.error(f"❌ {operation} failed with {client.name}: {str(e)}")

                # Log to database if available
                try:
                    if hasattr(self, 'db_manager'):
                        self.db_manager.log_error(
                            error_type='api_request_error',
                            error_message=str(e),
                            component=f'{client.name}_api',
                            severity='error'
                        )
                except:
                    pass  # Don't let logging errors break the flow

        # All attempts failed
        error_msg = f"All API clients failed for {operation}. Last error: {str(last_error)}"
        logger.error(error_msg)

        raise Exception(error_msg)

    def get_performance_report(self) -> Dict:
        """Get comprehensive performance report for all API clients"""
        report = {}

        for client_name, perf in self.client_performance.items():
            report[client_name] = {
                'requests': perf['requests'],
                'success_rate': (perf['successes'] / perf['requests']) if perf['requests'] > 0 else 0,
                'avg_response_time': perf['avg_response_time'],
                'consecutive_failures': perf['consecutive_failures'],
                'circuit_breaker_trips': perf['circuit_breaker_trips'],
                'health_score': self._calculate_client_score(client_name),
                'last_failure': perf['last_failure'].isoformat() if perf['last_failure'] else None,
                'last_success': perf['last_success'].isoformat() if 'last_success' in perf and perf['last_success'] else None
            }

        return report

    def reset_circuit_breakers(self):
        """Reset all circuit breakers and performance metrics"""
        for client_name in self.client_performance:
            perf = self.client_performance[client_name]
            perf['consecutive_failures'] = 0
            perf['circuit_breaker_trips'] = 0

        logger.info("Circuit breakers reset for all API clients")

    def get_client_status(self) -> Dict:
        """Get current status of all API clients"""
        status = {}

        for client in self.api_clients:
            perf = self.client_performance.get(client.name, {})
            status[client.name] = {
                'healthy': client.is_healthy(),
                'requests': perf.get('requests', 0),
                'success_rate': (perf.get('successes', 0) / perf.get('requests', 1)) if perf.get('requests', 0) > 0 else 0,
                'consecutive_failures': perf.get('consecutive_failures', 0)
            }

        return status