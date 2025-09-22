"""
Data Validator for Data Fetching Bot
Handles OHLCV data validation, quality scoring, and data integrity checks
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import re

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates OHLCV data integrity and quality"""

    def __init__(self):
        """Initialize data validator"""
        self.min_price = 0.00000001  # Minimum reasonable price
        self.max_price = 1000000000  # Maximum reasonable price
        self.min_volume = 0.0
        self.max_timestamp_deviation = timedelta(hours=1)  # Allow 1 hour deviation

    def validate_ohlcv_data(self, data: List[Dict], source_api: str = None) -> Dict:
        """Validate OHLCV data and calculate quality score"""
        if not data:
            return {
                'is_valid': False,
                'quality_score': 0.0,
                'errors': ['No data provided'],
                'warnings': []
            }

        try:
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(data)

            validation_results = {
                'is_valid': True,
                'quality_score': 1.0,
                'errors': [],
                'warnings': [],
                'data_points': len(df),
                'source_api': source_api
            }

            # Run individual validations
            format_valid = self._validate_format(df, validation_results)
            timestamp_valid = self._validate_timestamps(df, validation_results)
            values_valid = self._validate_values(df, validation_results)
            sequence_valid = self._validate_sequence(df, validation_results)

            # Calculate overall validity
            validation_results['is_valid'] = all([
                format_valid, timestamp_valid, values_valid, sequence_valid
            ])

            # Calculate quality score
            validation_results['quality_score'] = self._calculate_quality_score(validation_results)

            return validation_results

        except Exception as e:
            logger.error(f"Error during data validation: {e}")
            return {
                'is_valid': False,
                'quality_score': 0.0,
                'errors': [f'Validation error: {str(e)}'],
                'warnings': []
            }

    def _validate_format(self, df: pd.DataFrame, results: Dict) -> bool:
        """Validate data format and required columns"""
        required_columns = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}

        if not required_columns.issubset(df.columns):
            missing_cols = required_columns - set(df.columns)
            results['errors'].append(f"Missing required columns: {missing_cols}")
            return False

        # Check data types
        for col in required_columns:
            if col == 'timestamp':
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    results['errors'].append(f"Column '{col}' must be datetime type")
                    return False
            else:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    results['errors'].append(f"Column '{col}' must be numeric type")
                    return False

        return True

    def _validate_timestamps(self, df: pd.DataFrame, results: Dict) -> bool:
        """Validate timestamp integrity and chronological order"""
        try:
            # Check for duplicate timestamps
            duplicate_timestamps = df[df.duplicated(subset=['timestamp'], keep=False)]
            if not duplicate_timestamps.empty:
                results['warnings'].append(f"Found {len(duplicate_timestamps)} duplicate timestamps")
                # Keep only the first occurrence of each timestamp
                df = df.drop_duplicates(subset=['timestamp'], keep='first')

            # Check for chronological order
            if not df['timestamp'].is_monotonic_increasing:
                results['errors'].append("Timestamps are not in chronological order")
                return False

            # Check for reasonable time intervals (warn about large gaps)
            time_diffs = df['timestamp'].diff().dropna()
            large_gaps = time_diffs[time_diffs > timedelta(hours=1)]

            if not large_gaps.empty:
                results['warnings'].append(f"Found {len(large_gaps)} gaps larger than 1 hour")

            return True

        except Exception as e:
            results['errors'].append(f"Timestamp validation error: {str(e)}")
            return False

    def _validate_values(self, df: pd.DataFrame, results: Dict) -> bool:
        """Validate OHLCV values for reasonableness"""
        try:
            # Check for non-negative prices
            for col in ['open', 'high', 'low', 'close']:
                negative_prices = (df[col] < 0).sum()
                if negative_prices > 0:
                    results['errors'].append(f"Found {negative_prices} negative {col} prices")
                    return False

            # Check price ranges
            for col in ['open', 'high', 'low', 'close']:
                out_of_range = ((df[col] < self.min_price) | (df[col] > self.max_price)).sum()
                if out_of_range > 0:
                    results['warnings'].append(f"Found {out_of_range} {col} prices outside reasonable range")

            # Check OHLC relationship: high >= max(open, close), low <= min(open, close)
            ohlc_violations = (
                (df['high'] < df[['open', 'close']].max(axis=1)) |
                (df['low'] > df[['open', 'close']].min(axis=1))
            ).sum()

            if ohlc_violations > 0:
                results['errors'].append(f"Found {ohlc_violations} OHLC relationship violations")
                return False

            # Check for zero or negative volume
            zero_volume = (df['volume'] <= 0).sum()
            if zero_volume > 0:
                results['warnings'].append(f"Found {zero_volume} zero or negative volume entries")

            # Check for extreme price movements (more than 100% in one period)
            price_changes = abs(df['close'].pct_change()).dropna()
            extreme_changes = (price_changes > 1.0).sum()  # More than 100% change

            if extreme_changes > 0:
                results['warnings'].append(f"Found {extreme_changes} extreme price movements (>100%)")

            return True

        except Exception as e:
            results['errors'].append(f"Value validation error: {str(e)}")
            return False

    def _validate_sequence(self, df: pd.DataFrame, results: Dict) -> bool:
        """Validate data sequence and continuity"""
        try:
            # Check for missing data points
            if len(df) < 2:
                results['warnings'].append("Insufficient data points for sequence validation")
                return True

            # Calculate expected time intervals
            time_diffs = df['timestamp'].diff().dropna()
            most_common_interval = time_diffs.mode().iloc[0] if not time_diffs.empty else None

            if most_common_interval:
                try:
                    # Check for gaps in the sequence
                    freq_str = most_common_interval.freq if hasattr(most_common_interval, 'freq') else pd.infer_freq(time_diffs)
                    if freq_str:
                        expected_timestamps = pd.date_range(
                            start=df['timestamp'].min(),
                            end=df['timestamp'].max(),
                            freq=freq_str
                        )
                    else:
                        # If we can't infer frequency, just check for basic continuity
                        expected_timestamps = pd.date_range(
                            start=df['timestamp'].min(),
                            end=df['timestamp'].max(),
                            freq=time_diffs.iloc[0]  # Use the first interval
                        )
                except Exception:
                    # If frequency inference fails, skip this check
                    expected_timestamps = pd.DatetimeIndex([])

                actual_count = len(df)
                expected_count = len(expected_timestamps)

                if expected_count > actual_count:
                    missing_count = expected_count - actual_count
                    results['warnings'].append(f"Missing {missing_count} expected data points")

            return True

        except Exception as e:
            results['errors'].append(f"Sequence validation error: {str(e)}")
            return False

    def _calculate_quality_score(self, results: Dict) -> float:
        """Calculate overall data quality score"""
        base_score = 1.0

        # Penalize errors (critical)
        error_penalty = len(results['errors']) * 0.5
        base_score -= error_penalty

        # Penalize warnings (moderate)
        warning_penalty = len(results['warnings']) * 0.1
        base_score -= warning_penalty

        # Bonus for data completeness
        if results.get('data_points', 0) > 100:
            base_score += 0.05  # Bonus for larger datasets

        return max(0.0, min(1.0, base_score))

    def validate_single_record(self, record: Dict) -> Dict:
        """Validate a single data record"""
        try:
            # Basic format validation
            required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_fields = [field for field in required_fields if field not in record]

            if missing_fields:
                return {
                    'is_valid': False,
                    'quality_score': 0.0,
                    'errors': [f'Missing required fields: {missing_fields}'],
                    'warnings': []
                }

            # Type validation
            try:
                timestamp = pd.to_datetime(record['timestamp'])
                open_price = float(record['open'])
                high_price = float(record['high'])
                low_price = float(record['low'])
                close_price = float(record['close'])
                volume = float(record['volume'])
            except (ValueError, TypeError) as e:
                return {
                    'is_valid': False,
                    'quality_score': 0.0,
                    'errors': [f'Type conversion error: {str(e)}'],
                    'warnings': []
                }

            # Value validation
            errors = []
            warnings = []

            if any(price < 0 for price in [open_price, high_price, low_price, close_price]):
                errors.append('Negative price values')

            if volume < 0:
                warnings.append('Negative volume')

            if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
                errors.append('OHLC relationship violation')

            quality_score = 1.0
            if errors:
                quality_score -= 0.5
            if warnings:
                quality_score -= 0.1

            return {
                'is_valid': len(errors) == 0,
                'quality_score': max(0.0, quality_score),
                'errors': errors,
                'warnings': warnings
            }

        except Exception as e:
            return {
                'is_valid': False,
                'quality_score': 0.0,
                'errors': [f'Validation error: {str(e)}'],
                'warnings': []
            }

    def normalize_data(self, data: List[Dict], symbol: str = None) -> List[Dict]:
        """Normalize data to standard format"""
        normalized_data = []

        for record in data:
            try:
                # Ensure timestamp is datetime object
                timestamp = record.get('timestamp')
                if isinstance(timestamp, str):
                    timestamp = pd.to_datetime(timestamp).isoformat()
                elif hasattr(timestamp, 'isoformat'):
                    timestamp = timestamp.isoformat()

                # Ensure numeric values are floats
                normalized_record = {
                    'symbol': record.get('symbol', symbol),
                    'timestamp': timestamp,
                    'open': float(record.get('open', 0)),
                    'high': float(record.get('high', 0)),
                    'low': float(record.get('low', 0)),
                    'close': float(record.get('close', 0)),
                    'volume': float(record.get('volume', 0)),
                    'source_api': record.get('source_api', 'unknown'),
                    'data_quality_score': record.get('data_quality_score', 1.0),
                    'is_validated': record.get('is_validated', False)
                }

                normalized_data.append(normalized_record)

            except Exception as e:
                logger.error(f"Error normalizing record: {e}")
                continue

        return normalized_data

    def detect_anomalies(self, data: List[Dict], threshold: float = 3.0) -> List[Dict]:
        """Detect anomalous data points using statistical methods"""
        if not data or len(data) < 10:
            return []

        try:
            df = pd.DataFrame(data)

            # Calculate z-scores for price changes
            df['price_change'] = df['close'].pct_change()
            df['price_zscore'] = (df['price_change'] - df['price_change'].mean()) / df['price_change'].std()

            # Calculate z-scores for volume
            df['volume_zscore'] = (df['volume'] - df['volume'].mean()) / df['volume'].std()

            # Find anomalies
            anomalies = []
            for idx, row in df.iterrows():
                if abs(row.get('price_zscore', 0)) > threshold or abs(row.get('volume_zscore', 0)) > threshold:
                    anomalies.append({
                        'index': idx,
                        'timestamp': row['timestamp'],
                        'price_zscore': row.get('price_zscore', 0),
                        'volume_zscore': row.get('volume_zscore', 0),
                        'close_price': row['close'],
                        'volume': row['volume']
                    })

            return anomalies

        except Exception as e:
            logger.error(f"Error detecting anomalies: {e}")
            return []