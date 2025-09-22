"""
Gap Detector for Data Fetching Bot
Detects missing data periods and gaps in cryptocurrency data
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from collections import defaultdict

logger = logging.getLogger(__name__)


class GapDetector:
    """Detects gaps in cryptocurrency data and analyzes data completeness"""

    def __init__(self, db_manager):
        """Initialize gap detector with database manager"""
        self.db_manager = db_manager

    def detect_gaps(self, symbol: str = None, days_back: int = 30) -> List[Dict]:
        """Detect gaps in data for specified symbol or all symbols"""
        logger.info(f"🔍 Detecting gaps in data (symbol: {symbol or 'all'}, days_back: {days_back})")

        try:
            # Get data coverage from database
            db_info = self.db_manager.get_database_info()
            data_coverage = db_info.get('data_coverage', [])

            if not data_coverage:
                logger.warning("No data coverage information available")
                return []

            gaps = []

            # If symbol specified, filter for that symbol
            if symbol:
                data_coverage = [item for item in data_coverage if item[0] == symbol]

            # Analyze each symbol
            for coverage_item in data_coverage:
                symbol_name = coverage_item[0]
                min_date = coverage_item[2]
                max_date = coverage_item[3]

                if not min_date or not max_date:
                    logger.warning(f"No date range available for {symbol_name}")
                    continue

                symbol_gaps = self._detect_symbol_gaps(symbol_name, min_date, max_date, days_back)
                gaps.extend(symbol_gaps)

            logger.info(f"✅ Gap detection completed. Found {len(gaps)} gaps")
            return gaps

        except Exception as e:
            logger.error(f"Error during gap detection: {e}")
            return []

    def _detect_symbol_gaps(self, symbol: str, min_date: str, max_date: str, days_back: int) -> List[Dict]:
        """Detect gaps for a specific symbol"""
        gaps = []

        try:
            # Convert date strings to datetime
            start_date = pd.to_datetime(min_date)
            end_date = pd.to_datetime(max_date)
            cutoff_date = datetime.now() - timedelta(days=days_back)

            # Only analyze recent data
            analysis_start = max(start_date, cutoff_date)

            # Get existing data points for this symbol
            with sqlite3.connect(self.db_manager.db_path) as conn:
                cursor = conn.execute("""
                    SELECT timestamp
                    FROM crypto_data
                    WHERE symbol = ?
                    AND timestamp >= ?
                    ORDER BY timestamp
                """, (symbol, analysis_start.isoformat()))

                timestamps = [row[0] for row in cursor.fetchall()]

            if not timestamps:
                logger.warning(f"No recent data points found for {symbol}")
                return []

            # Convert to datetime objects
            timestamps = pd.to_datetime(timestamps)

            # Get expected time intervals based on most common interval
            time_diffs = timestamps.diff().dropna()
            if time_diffs.empty:
                return []

            # Use the most common interval (mode)
            most_common_interval = time_diffs.mode()
            if most_common_interval.empty:
                logger.warning(f"Could not determine time interval for {symbol}")
                return []

            expected_interval = most_common_interval.iloc[0]

            # Find gaps where actual interval > expected interval
            gap_indices = []
            for i in range(1, len(timestamps)):
                actual_interval = timestamps.iloc[i] - timestamps.iloc[i-1]
                if actual_interval > expected_interval + timedelta(minutes=10):  # Allow 10 min tolerance
                    gap_indices.append(i)

            # Create gap records
            for gap_idx in gap_indices:
                gap_start = timestamps.iloc[gap_idx - 1]
                gap_end = timestamps.iloc[gap_idx]
                gap_duration = gap_end - gap_start

                gap_record = {
                    'symbol': symbol,
                    'gap_start': gap_start.isoformat(),
                    'gap_end': gap_end.isoformat(),
                    'gap_duration_hours': gap_duration.total_seconds() / 3600,
                    'expected_interval_minutes': expected_interval.total_seconds() / 60,
                    'severity': 'high' if gap_duration > timedelta(hours=1) else 'medium' if gap_duration > timedelta(minutes=30) else 'low',
                    'detected_at': datetime.now().isoformat()
                }

                gaps.append(gap_record)

                # Log significant gaps
                if gap_duration > timedelta(hours=1):
                    logger.warning(f"🚨 Large gap detected in {symbol}: {gap_duration} (from {gap_start} to {gap_end})")

            logger.info(f"Found {len(gaps)} gaps for {symbol}")
            return gaps

        except Exception as e:
            logger.error(f"Error detecting gaps for {symbol}: {e}")
            return []

    def analyze_data_completeness(self, symbol: str = None, days_back: int = 7) -> Dict:
        """Analyze overall data completeness and quality metrics"""
        logger.info(f"📊 Analyzing data completeness (symbol: {symbol or 'all'}, days_back: {days_back})")

        try:
            db_info = self.db_manager.get_database_info()
            data_coverage = db_info.get('data_coverage', [])

            if not data_coverage:
                return {'error': 'No data available for analysis'}

            analysis = {
                'total_symbols': len(data_coverage),
                'total_records': db_info.get('total_records', 0),
                'symbols_analyzed': 0,
                'completeness_scores': {},
                'gap_summary': {},
                'quality_issues': []
            }

            for coverage_item in data_coverage:
                symbol_name = coverage_item[0]

                # Skip if specific symbol requested
                if symbol and symbol_name != symbol:
                    continue

                analysis['symbols_analyzed'] += 1

                # Calculate completeness score
                completeness_score = self._calculate_completeness_score(symbol_name, days_back)
                analysis['completeness_scores'][symbol_name] = completeness_score

                # Get gap summary
                gaps = self.detect_gaps(symbol_name, days_back)
                analysis['gap_summary'][symbol_name] = {
                    'total_gaps': len(gaps),
                    'total_gap_hours': sum(gap.get('gap_duration_hours', 0) for gap in gaps),
                    'max_gap_hours': max((gap.get('gap_duration_hours', 0) for gap in gaps), default=0)
                }

            # Calculate overall metrics
            if analysis['completeness_scores']:
                analysis['average_completeness'] = sum(analysis['completeness_scores'].values()) / len(analysis['completeness_scores'])
                analysis['perfect_symbols'] = sum(1 for score in analysis['completeness_scores'].values() if score >= 0.95)

            logger.info(f"✅ Data completeness analysis completed for {analysis['symbols_analyzed']} symbols")
            return analysis

        except Exception as e:
            logger.error(f"Error during completeness analysis: {e}")
            return {'error': str(e)}

    def _calculate_completeness_score(self, symbol: str, days_back: int) -> float:
        """Calculate completeness score for a symbol over specified period"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_back)

            # Get data points in the period
            with self.db_manager._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) as data_points,
                           MIN(timestamp) as min_date,
                           MAX(timestamp) as max_date
                    FROM crypto_data
                    WHERE symbol = ?
                    AND timestamp >= ?
                """, (symbol, cutoff_date.isoformat()))

                result = cursor.fetchone()

            if not result or result[0] == 0:
                return 0.0

            data_points = result[0]

            # Calculate expected number of data points
            # Assuming hourly data for now
            min_date = pd.to_datetime(result[1])
            max_date = pd.to_datetime(result[2])
            time_span = max_date - min_date

            if time_span.total_seconds() <= 0:
                return 0.0

            expected_hours = time_span.total_seconds() / 3600
            expected_points = expected_hours  # One point per hour

            # Add some tolerance for missing data
            completeness_ratio = min(data_points / expected_points, 1.0)

            # Bonus for recent data
            hours_since_last = (datetime.now() - max_date).total_seconds() / 3600
            recency_bonus = max(0, 1.0 - (hours_since_last / 24))  # Full bonus if data < 24 hours old

            # Penalize for gaps
            gaps = self.detect_gaps(symbol, days_back)
            gap_penalty = min(0.3, len(gaps) * 0.05)  # Max 30% penalty

            final_score = (completeness_ratio * 0.7 + recency_bonus * 0.3) - gap_penalty

            return max(0.0, min(1.0, final_score))

        except Exception as e:
            logger.error(f"Error calculating completeness for {symbol}: {e}")
            return 0.0

    def get_gap_report(self, symbol: str = None, days_back: int = 30) -> Dict:
        """Generate comprehensive gap report"""
        gaps = self.detect_gaps(symbol, days_back)
        completeness = self.analyze_data_completeness(symbol, days_back)

        report = {
            'summary': {
                'total_gaps': len(gaps),
                'symbols_affected': len(set(gap['symbol'] for gap in gaps)) if gaps else 0,
                'total_gap_hours': sum(gap.get('gap_duration_hours', 0) for gap in gaps),
                'average_gap_hours': (sum(gap.get('gap_duration_hours', 0) for gap in gaps) / len(gaps)) if gaps else 0,
                'max_gap_hours': max((gap.get('gap_duration_hours', 0) for gap in gaps), default=0)
            },
            'gaps': gaps,
            'completeness_analysis': completeness,
            'generated_at': datetime.now().isoformat()
        }

        # Categorize gaps by severity
        report['summary']['severity_breakdown'] = {
            'high': len([g for g in gaps if g.get('severity') == 'high']),
            'medium': len([g for g in gaps if g.get('severity') == 'medium']),
            'low': len([g for g in gaps if g.get('severity') == 'low'])
        }

        logger.info(f"✅ Gap report generated with {len(gaps)} gaps found")
        return report