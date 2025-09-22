"""
Retry Manager for Data Fetching Bot
Implements retry mechanisms with exponential backoff and circuit breaker patterns
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Different retry strategies"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    IMMEDIATE = "immediate"


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Failing, requests blocked
    HALF_OPEN = "half_open" # Testing if service recovered


class RetryManager:
    """Manages retry logic with exponential backoff and circuit breaker patterns"""

    def __init__(self,
                 max_retries: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                 jitter: bool = True):
        """Initialize retry manager"""
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.strategy = strategy
        self.jitter = jitter

        # Circuit breaker configuration
        self.circuit_breaker_threshold = 5  # failures before opening circuit
        self.circuit_breaker_timeout = 60   # seconds to wait before half-open
        self.circuit_breaker_window = 300  # seconds to track failures

        # Track failures per component
        self.failure_counts = {}
        self.circuit_breakers = {}
        self.last_failure_times = {}

        logger.info("Retry Manager initialized with exponential backoff and circuit breaker")

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt"""
        if self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = min(self.base_delay * (attempt + 1), self.max_delay)
        elif self.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.base_delay
        else:  # IMMEDIATE
            delay = 0

        # Add jitter to prevent thundering herd
        if self.jitter and delay > 0:
            jitter_amount = delay * 0.1 * random.random()
            delay += jitter_amount

        return delay

    def _is_circuit_open(self, component: str) -> bool:
        """Check if circuit breaker is open for a component"""
        if component not in self.circuit_breakers:
            return False

        cb_state, last_failure = self.circuit_breakers[component], self.last_failure_times.get(component, datetime.min)

        if cb_state == CircuitBreakerState.OPEN:
            # Check if we should transition to half-open
            time_since_failure = (datetime.now() - last_failure).total_seconds()
            if time_since_failure >= self.circuit_breaker_timeout:
                self.circuit_breakers[component] = CircuitBreakerState.HALF_OPEN
                logger.info(f"Circuit breaker for {component} transitioning to HALF_OPEN")
                return False
            return True

        return False

    def _record_failure(self, component: str):
        """Record a failure for circuit breaker tracking"""
        current_time = datetime.now()

        # Initialize if needed
        if component not in self.failure_counts:
            self.failure_counts[component] = 0
            self.circuit_breakers[component] = CircuitBreakerState.CLOSED

        # Reset count if outside window
        if component in self.last_failure_times:
            time_since_last = (current_time - self.last_failure_times[component]).total_seconds()
            if time_since_last > self.circuit_breaker_window:
                self.failure_counts[component] = 0

        # Record failure
        self.failure_counts[component] += 1
        self.last_failure_times[component] = current_time

        # Check if we should open circuit breaker
        if self.failure_counts[component] >= self.circuit_breaker_threshold:
            self.circuit_breakers[component] = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker OPENED for {component} after {self.failure_counts[component]} failures")

    def _record_success(self, component: str):
        """Record a success and potentially close circuit breaker"""
        if component in self.circuit_breakers:
            if self.circuit_breakers[component] == CircuitBreakerState.HALF_OPEN:
                # Success in half-open state, close the circuit
                self.circuit_breakers[component] = CircuitBreakerState.CLOSED
                self.failure_counts[component] = 0
                logger.info(f"Circuit breaker CLOSED for {component} after successful test")

            elif self.circuit_breakers[component] == CircuitBreakerState.CLOSED:
                # Reset failure count on success
                self.failure_counts[component] = max(0, self.failure_counts[component] - 1)

    async def execute_with_retry(self,
                                operation: Callable,
                                component: str = "default",
                                **kwargs) -> Any:
        """Execute operation with retry logic and circuit breaker"""

        # Check circuit breaker
        if self._is_circuit_open(component):
            raise Exception(f"Circuit breaker is OPEN for {component}")

        last_error = None

        for attempt in range(self.max_retries + 1):  # +1 for initial attempt
            try:
                # Execute operation
                start_time = datetime.now()
                result = await operation(**kwargs)
                execution_time = (datetime.now() - start_time).total_seconds()

                # Record success
                self._record_success(component)

                if attempt > 0:
                    logger.info(f"✅ Operation succeeded on attempt {attempt + 1} for {component}")

                return result

            except Exception as e:
                last_error = e
                self._record_failure(component)

                # If this was the last attempt, don't retry
                if attempt == self.max_retries:
                    logger.error(f"❌ All {self.max_retries + 1} attempts failed for {component}: {str(e)}")
                    break

                # Calculate and wait before retry
                delay = self._calculate_delay(attempt)

                if delay > 0:
                    logger.warning(f"⚠️ Attempt {attempt + 1} failed for {component}, retrying in {delay".1f"}s: {str(e)}")
                    await asyncio.sleep(delay)
                else:
                    logger.warning(f"⚠️ Attempt {attempt + 1} failed for {component}, retrying immediately: {str(e)}")

        # All retries exhausted
        error_msg = f"Operation failed after {self.max_retries + 1} attempts for {component}. Last error: {str(last_error)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    def get_circuit_breaker_status(self) -> Dict:
        """Get status of all circuit breakers"""
        status = {}

        for component, state in self.circuit_breakers.items():
            failure_count = self.failure_counts.get(component, 0)
            last_failure = self.last_failure_times.get(component)

            status[component] = {
                'state': state.value,
                'failure_count': failure_count,
                'last_failure': last_failure.isoformat() if last_failure else None,
                'is_open': state == CircuitBreakerState.OPEN,
                'is_half_open': state == CircuitBreakerState.HALF_OPEN
            }

        return status

    def reset_circuit_breaker(self, component: str):
        """Manually reset circuit breaker for a component"""
        if component in self.circuit_breakers:
            self.circuit_breakers[component] = CircuitBreakerState.CLOSED
            self.failure_counts[component] = 0
            logger.info(f"Circuit breaker manually reset for {component}")

    def reset_all_circuit_breakers(self):
        """Reset all circuit breakers"""
        for component in self.circuit_breakers:
            self.circuit_breakers[component] = CircuitBreakerState.CLOSED
            self.failure_counts[component] = 0

        logger.info("All circuit breakers reset")