# API Integration Guide

## 📋 Document Information

- **Document Version**: 1.0.0
- **Created**: 2025-09-21
- **Last Updated**: 2025-09-21
- **Status**: Active
- **Author**: Data-Factory Technical Team

---

## 🎯 Overview

This guide provides comprehensive information about API integrations used in the Data-Factory system for cryptocurrency data acquisition. The system supports multiple APIs with automatic failover and load balancing capabilities.

## 📊 Supported APIs

### Primary APIs

#### 1. Binance API
- **Status**: ✅ Fully Implemented
- **Rate Limit**: 1200 requests/minute (with API key)
- **Authentication**: API key required for enhanced limits
- **Data Types**: Historical klines, ticker data, order book
- **Reliability**: High (99.9% uptime)

**Endpoints Used:**
```python
# Historical Klines
GET /api/v3/klines

# 24hr Ticker Price Change Statistics
GET /api/v3/ticker/24hr

# Exchange Information
GET /api/v3/exchangeInfo
```

**Implementation Status:**
- [x] Basic authentication
- [x] Historical data fetching
- [x] Rate limiting compliance
- [ ] WebSocket integration
- [ ] Order book data

#### 2. CoinGecko API
- **Status**: ✅ Partially Implemented
- **Rate Limit**: 10-50 requests/minute (free tier)
- **Authentication**: Free tier available
- **Data Types**: Market data, historical prices, coin information
- **Reliability**: Medium (occasional 401 errors)

**Endpoints Used:**
```python
# Get Coin Market Chart by ID
GET /api/v3/coins/{id}/market_chart/range

# Get Simple Price
GET /api/v3/simple/price

# Get Coin List
GET /api/v3/coins/list
```

**Implementation Status:**
- [x] Basic price fetching
- [ ] Historical data integration
- [ ] Rate limiting optimization
- [ ] Error handling improvements

#### 3. Yahoo Finance
- **Status**: ✅ Partially Implemented
- **Rate Limit**: Variable (depends on usage patterns)
- **Authentication**: None required
- **Data Types**: Historical prices, market data
- **Reliability**: Medium (occasional throttling)

**Implementation:**
- Uses `yfinance` Python library
- Historical data via download method
- Supports multiple timeframes

**Implementation Status:**
- [x] Basic integration via yfinance
- [ ] Column mapping optimization
- [ ] Error handling improvements
- [ ] Rate limiting compliance

### Backup APIs

#### 4. CoinMarketCap API
- **Status**: ⏳ Planned
- **Rate Limit**: 10 requests/minute (free tier)
- **Authentication**: API key required
- **Data Types**: Comprehensive market data
- **Reliability**: High

**Planned Implementation:**
```python
# Cryptocurrency Listings Latest
GET /v1/cryptocurrency/listings/latest

# Cryptocurrency Quotes Historical
GET /v2/cryptocurrency/quotes/historical
```

#### 5. Kraken API
- **Status**: ⏳ Planned
- **Rate Limit**: 1 request/second (public endpoints)
- **Authentication**: None for public data
- **Data Types**: OHLC data, ticker information
- **Reliability**: High

#### 6. Coinbase Pro API
- **Status**: ⏳ Planned
- **Rate Limit**: 3 requests/second (public endpoints)
- **Authentication**: None for public data
- **Data Types**: Historical rates, products
- **Reliability**: Medium

## 🔧 API Client Architecture

### BaseAPIClient (Abstract Base Class)

```python
class BaseAPIClient:
    """Abstract base class for all API clients"""

    def __init__(self, name: str, rate_limit: int):
        self.name = name
        self.rate_limit = rate_limit
        self.request_count = 0
        self.last_request_time = None
        self.health_status = "healthy"
        self.error_count = 0

    async def make_request(self, endpoint: str, params: dict) -> dict:
        """Make rate-limited API request with error handling"""
        await self._enforce_rate_limit()
        response = await self._execute_request(endpoint, params)
        self._update_health_status(response)
        return response

    def is_healthy(self) -> bool:
        """Check if API is healthy for requests"""
        return self.health_status == "healthy"
```

### BinanceAPIClient

```python
class BinanceAPIClient(BaseAPIClient):
    """Binance API client implementation"""

    def __init__(self):
        super().__init__("Binance", 1200)
        self.client = Client(api_key=os.getenv('BINANCE_API_KEY'))

    async def get_historical_klines(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str = None
    ) -> pd.DataFrame:
        """Fetch historical klines data"""
        # Implementation details...
        pass
```

### CoinGeckoAPIClient

```python
class CoinGeckoAPIClient(BaseAPIClient):
    """CoinGecko API client implementation"""

    def __init__(self):
        super().__init__("CoinGecko", 50)
        self.base_url = "https://api.coingecko.com/api/v3"

    async def get_market_chart(
        self,
        coin_id: str,
        vs_currency: str,
        start_date: int,
        end_date: int
    ) -> pd.DataFrame:
        """Fetch market chart data"""
        # Implementation details...
        pass
```

## 📊 Rate Limiting Management

### Rate Limiter Implementation

```python
class RateLimiter:
    """Manages API rate limits across all clients"""

    def __init__(self):
        self.api_limits = {
            'binance': 1200,      # requests per minute
            'coingecko': 50,      # requests per minute
            'yahoo': 100,         # requests per minute
            'coinmarketcap': 10   # requests per minute
        }
        self.request_history = defaultdict(list)

    async def acquire_token(self, api_name: str) -> bool:
        """Acquire permission to make API request"""
        now = time.time()
        minute_ago = now - 60

        # Clean old requests
        self.request_history[api_name] = [
            req_time for req_time in self.request_history[api_name]
            if req_time > minute_ago
        ]

        # Check if under limit
        if len(self.request_history[api_name]) < self.api_limits[api_name]:
            self.request_history[api_name].append(now)
            return True

        return False
```

### Request Throttling

```python
class RequestThrottler:
    """Intelligent request throttling based on API health"""

    def __init__(self):
        self.api_health = {}
        self.consecutive_errors = defaultdict(int)

    async def calculate_delay(self, api_name: str) -> float:
        """Calculate appropriate delay between requests"""
        base_delay = 1.0  # seconds

        if self.api_health.get(api_name) == "degraded":
            base_delay *= 2  # Double delay for degraded APIs

        if self.consecutive_errors[api_name] > 5:
            base_delay *= min(10, self.consecutive_errors[api_name])  # Exponential backoff

        return base_delay
```

## 🔄 Error Handling & Recovery

### Error Classification

```python
class APIError(Exception):
    """Base exception for API errors"""

    def __init__(self, message: str, error_code: int = None, api_name: str = None):
        super().__init__(message)
        self.error_code = error_code
        self.api_name = api_name

class RateLimitError(APIError):
    """Rate limit exceeded"""
    pass

class AuthenticationError(APIError):
    """Authentication failure"""
    pass

class NetworkError(APIError):
    """Network connectivity issues"""
    pass

class DataFormatError(APIError):
    """Invalid data format received"""
    pass
```

### Retry Strategy

```python
class RetryManager:
    """Manages retry logic with exponential backoff"""

    def __init__(self):
        self.max_retries = 5
        self.base_delay = 1.0  # seconds
        self.max_delay = 300.0  # 5 minutes

    async def execute_with_retry(self, func, *args, **kwargs):
        """Execute function with retry logic"""
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except (RateLimitError, NetworkError) as e:
                last_exception = e

                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt, e)
                    await asyncio.sleep(delay)
                    continue

            except (AuthenticationError, DataFormatError):
                # Don't retry for these errors
                raise

        raise last_exception

    def _calculate_delay(self, attempt: int, error: Exception) -> float:
        """Calculate delay for retry"""
        if isinstance(error, RateLimitError):
            # Longer delay for rate limit errors
            delay = min(self.base_delay * (2 ** attempt) * 2, self.max_delay)
        else:
            # Standard exponential backoff
            delay = min(self.base_delay * (2 ** attempt), self.max_delay)

        # Add jitter to prevent thundering herd
        jitter = delay * 0.1 * (0.5 + 0.5 * random.random())
        return delay + jitter
```

## 📈 Performance Monitoring

### Metrics Collection

```python
class APIMetrics:
    """Collects and reports API performance metrics"""

    def __init__(self):
        self.metrics = defaultdict(lambda: {
            'request_count': 0,
            'success_count': 0,
            'error_count': 0,
            'total_response_time': 0.0,
            'last_request_time': None
        })

    def record_request(self, api_name: str, success: bool, response_time: float):
        """Record API request metrics"""
        metrics = self.metrics[api_name]
        metrics['request_count'] += 1

        if success:
            metrics['success_count'] += 1
        else:
            metrics['error_count'] += 1

        metrics['total_response_time'] += response_time
        metrics['last_request_time'] = time.time()

    def get_success_rate(self, api_name: str) -> float:
        """Get success rate for API"""
        metrics = self.metrics[api_name]
        total_requests = metrics['request_count']

        if total_requests == 0:
            return 0.0

        return metrics['success_count'] / total_requests

    def get_average_response_time(self, api_name: str) -> float:
        """Get average response time for API"""
        metrics = self.metrics[api_name]
        total_requests = metrics['request_count']

        if total_requests == 0:
            return 0.0

        return metrics['total_response_time'] / total_requests
```

## 🔒 Security Considerations

### API Key Management
```python
class SecureAPIKeyManager:
    """Securely manages API keys"""

    def __init__(self):
        self.key_file = os.path.expanduser("~/.crypto_api_keys")
        self.keys = {}

    def get_api_key(self, api_name: str) -> Optional[str]:
        """Get API key for service"""
        if api_name not in self.keys:
            self.keys[api_name] = self._load_key(api_name)

        return self.keys[api_name]

    def _load_key(self, api_name: str) -> Optional[str]:
        """Load API key from secure storage"""
        env_key = f"{api_name.upper()}_API_KEY"

        # Try environment variable first
        key = os.getenv(env_key)
        if key:
            return key

        # Try key file
        if os.path.exists(self.key_file):
            with open(self.key_file, 'r') as f:
                for line in f:
                    if line.startswith(f"{api_name}:"):
                        return line.split(':', 1)[1].strip()

        return None
```

### Access Control
- API keys stored as environment variables
- Encrypted key file as backup
- Access logging for all API requests
- Rate limit monitoring to prevent abuse

## 🚀 Best Practices

### 1. API Usage Guidelines
- Always check API health before making requests
- Implement exponential backoff for retries
- Respect rate limits to avoid account suspension
- Monitor error rates and adjust accordingly

### 2. Error Handling
- Classify errors by type for appropriate handling
- Implement circuit breaker pattern for failing APIs
- Log detailed error information for debugging
- Provide fallback mechanisms for critical operations

### 3. Performance Optimization
- Use connection pooling for HTTP requests
- Implement request caching where appropriate
- Monitor response times and optimize slow endpoints
- Use asynchronous processing for concurrent requests

### 4. Data Quality Assurance
- Validate data integrity after each API call
- Cross-reference data from multiple APIs when possible
- Implement anomaly detection for suspicious data
- Maintain data consistency across all sources

## 📊 Monitoring & Alerts

### Health Checks
```python
async def check_api_health(api_client: BaseAPIClient) -> dict:
    """Perform comprehensive API health check"""
    health_status = {
        'api_name': api_client.name,
        'is_healthy': False,
        'response_time': None,
        'last_error': None,
        'consecutive_failures': api_client.error_count
    }

    try:
        start_time = time.time()
        # Make test request
        test_response = await api_client.make_request('/ping', {})
        response_time = time.time() - start_time

        health_status.update({
            'is_healthy': True,
            'response_time': response_time
        })

    except Exception as e:
        health_status['last_error'] = str(e)

    return health_status
```

### Alerting System
- API failure alerts via email/Slack
- Rate limit warnings
- Performance degradation notifications
- Data quality issue alerts

---

## 📋 Implementation Checklist

### ✅ Completed
- [x] BaseAPIClient abstract class
- [x] BinanceAPIClient implementation
- [x] Basic rate limiting
- [x] Error handling framework
- [x] Metrics collection

### 🔄 In Progress
- [ ] CoinGeckoAPIClient completion
- [ ] Yahoo Finance integration
- [ ] Advanced retry mechanisms
- [ ] Load balancing system

### 📋 Planned
- [ ] CoinMarketCap integration
- [ ] Kraken API client
- [ ] Coinbase Pro integration
- [ ] Real-time WebSocket support

---

*This guide ensures reliable and efficient API integration for the Data-Factory system.*

**Document Status**: ✅ **ACTIVE** | Version: 1.0.0 | Last Updated: 2025-09-21
