"""
Backfill Manager for Data Fetching Bot
Handles historical data recovery for identified gaps
"""

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd

logger = logging.getLogger(__name__)


class BackfillManager:
    """Manages historical data recovery and gap filling"""

    def __init__(self, api_manager, db_manager, retry_manager):
        """Initialize backfill manager"""
        self.api_manager = api_manager
        self.db_manager = db_manager
        self.retry_manager = retry_manager

        # Backfill configuration
        self.max_concurrent_backfills = 3
        self.chunk_size_hours = 24 * 30  # 30 days per chunk
        self.rate_limit_delay = 1.0  # seconds between chunks
        self.max_backfill_age_days = 365 * 2  # Don't backfill data older than 2 years

    async def backfill_gaps(self, gaps: List[Dict], priority: str = 'auto') -> Dict:
        """Backfill multiple gaps with intelligent prioritization"""
        logger.info(f"🔄 Starting backfill process for {len(gaps)} gaps")

        if not gaps:
            return {'success': True, 'message': 'No gaps to backfill'}

        # Sort gaps by priority
        prioritized_gaps = self._prioritize_gaps(gaps, priority)

        results = {
            'total_gaps': len(gaps),
            'processed_gaps': 0,
            'successful_backfills': 0,
            'failed_backfills': 0,
            'total_records_added': 0,
            'errors': [],
            'details': []
        }

        # Process gaps concurrently with limit
        semaphore = asyncio.Semaphore(self.max_concurrent_backfills)

        async def process_gap(gap):
            async with semaphore:
                return await self._backfill_single_gap(gap)

        # Execute backfill tasks
        tasks = [process_gap(gap) for gap in prioritized_gaps[:10]]  # Limit to first 10 for now
        gap_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(gap_results):
            if isinstance(result, Exception):
                logger.error(f"Backfill failed for gap {i}: {result}")
                results['errors'].append(str(result))
                results['failed_backfills'] += 1
            else:
                results['processed_gaps'] += 1
                results['total_records_added'] += result.get('records_added', 0)
                results['details'].append(result)

                if result.get('success', False):
                    results['successful_backfills'] += 1
                else:
                    results['failed_backfills'] += 1

        logger.info(f"✅ Backfill process completed: {results['successful_backfills']}/{results['processed_gaps']} successful")
        return results

    def _prioritize_gaps(self, gaps: List[Dict], priority: str = 'auto') -> List[Dict]:
        """Sort gaps by priority for backfilling"""
        if priority == 'auto':
            # Sort by: recency (newer gaps first), then size (smaller gaps first), then severity
            def sort_key(gap):
                gap_start = pd.to_datetime(gap['gap_start'])
                gap_duration = gap.get('gap_duration_hours', 0)
                severity_score = {'low': 1, 'medium': 2, 'high': 3}.get(gap.get('severity', 'medium'), 2)

                # Recent gaps get higher priority
                recency_score = (datetime.now() - gap_start).total_seconds()

                return (-recency_score, gap_duration, severity_score)

            return sorted(gaps, key=sort_key)
        elif priority == 'size_asc':
            return sorted(gaps, key=lambda x: x.get('gap_duration_hours', 0))
        elif priority == 'size_desc':
            return sorted(gaps, key=lambda x: x.get('gap_duration_hours', 0), reverse=True)
        elif priority == 'severity':
            severity_order = {'high': 0, 'medium': 1, 'low': 2}
            return sorted(gaps, key=lambda x: severity_order.get(x.get('severity', 'medium'), 1))
        else:
            return gaps

    async def _backfill_single_gap(self, gap: Dict) -> Dict:
        """Backfill a single gap in data"""
        symbol = gap['symbol']
        gap_start = pd.to_datetime(gap['gap_start'])
        gap_end = pd.to_datetime(gap['gap_end'])

        logger.info(f"📜 Backfilling gap for {symbol}: {gap_start} to {gap_end}")

        result = {
            'symbol': symbol,
            'gap_start': gap_start.isoformat(),
            'gap_end': gap_end.isoformat(),
            'success': False,
            'records_added': 0,
            'error': None,
            'chunks_processed': 0
        }

        try:
            # Check if gap is too old
            age_days = (datetime.now() - gap_start).days
            if age_days > self.max_backfill_age_days:
                result['error'] = f'Gap too old ({age_days} days > {self.max_backfill_age_days} days limit)'
                return result

            # Split gap into manageable chunks
            chunks = self._create_backfill_chunks(gap_start, gap_end)

            total_records = 0
            successful_chunks = 0

            for chunk_start, chunk_end in chunks:
                try:
                    # Fetch data for this chunk
                    records = await self._fetch_chunk_data(symbol, chunk_start, chunk_end)

                    if records:
                        # Validate and insert data
                        validated_data = self._validate_chunk_data(records, symbol)
                        inserted_count = self.db_manager.insert_crypto_data(validated_data)
                        total_records += inserted_count

                        logger.info(f"✅ Backfilled {inserted_count} records for {symbol} chunk {chunk_start} to {chunk_end}")
                        successful_chunks += 1
                    else:
                        logger.warning(f"⚠️ No data returned for {symbol} chunk {chunk_start} to {chunk_end}")

                    # Rate limiting between chunks
                    await asyncio.sleep(self.rate_limit_delay)

                except Exception as e:
                    logger.error(f"❌ Failed to backfill chunk {chunk_start} to {chunk_end} for {symbol}: {e}")
                    result['error'] = str(e)
                    continue

                result['chunks_processed'] = len(chunks)

            # Update gap status in database
            try:
                self._update_gap_status(gap, total_records > 0)
            except Exception as e:
                logger.error(f"Failed to update gap status: {e}")

            result['success'] = total_records > 0
            result['records_added'] = total_records

            if result['success']:
                logger.info(f"✅ Successfully backfilled {total_records} records for {symbol} gap")
            else:
                logger.warning(f"⚠️ Failed to backfill any data for {symbol} gap")

            return result

        except Exception as e:
            logger.error(f"❌ Backfill failed for {symbol}: {e}")
            result['error'] = str(e)
            return result

    def _create_backfill_chunks(self, gap_start: datetime, gap_end: datetime) -> List[Tuple[datetime, datetime]]:
        """Split gap into manageable time chunks"""
        chunks = []
        current_start = gap_start

        while current_start < gap_end:
            chunk_end = min(current_start + timedelta(hours=self.chunk_size_hours), gap_end)
            chunks.append((current_start, chunk_end))
            current_start = chunk_end

        logger.info(f"Split gap into {len(chunks)} chunks of max {self.chunk_size_hours} hours each")
        return chunks

    async def _fetch_chunk_data(self, symbol: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Fetch data for a specific time chunk"""
        try:
            # Use API manager to fetch historical data
            data = await self.api_manager.make_request(
                'get_historical_klines_batch',
                symbol=symbol,
                interval='1h',
                start_date=start_time.isoformat(),
                end_date=end_time.isoformat(),
                max_requests=50  # Limit requests per chunk
            )

            return data if data else []

        except Exception as e:
            logger.error(f"Failed to fetch chunk data for {symbol}: {e}")
            return []

    def _validate_chunk_data(self, data: List[Dict], symbol: str) -> List[Dict]:
        """Validate and normalize chunk data"""
        if not data:
            return []

        try:
            # Add symbol to each record if missing
            for record in data:
                if 'symbol' not in record:
                    record['symbol'] = symbol

            # Basic validation - can be enhanced with full validation
            validated_records = []
            for record in data:
                try:
                    # Basic checks
                    if all(key in record for key in ['timestamp', 'open', 'high', 'low', 'close', 'volume']):
                        # Convert timestamp if needed
                        if isinstance(record['timestamp'], str):
                            record['timestamp'] = pd.to_datetime(record['timestamp']).isoformat()

                        # Ensure numeric values
                        for key in ['open', 'high', 'low', 'close', 'volume']:
                            record[key] = float(record[key])

                        validated_records.append(record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Invalid record skipped: {e}")
                    continue

            return validated_records

        except Exception as e:
            logger.error(f"Error validating chunk data: {e}")
            return []

    def _update_gap_status(self, gap: Dict, backfilled: bool):
        """Update gap status in database"""
        try:
            # Insert or update gap record
            with sqlite3.connect(self.db_manager.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO data_gaps
                    (symbol, gap_start, gap_end, gap_duration_hours, recovery_status, recovery_attempts, last_recovery_attempt)
                    VALUES (?, ?, ?, ?, ?, COALESCE(
                        (SELECT recovery_attempts FROM data_gaps WHERE symbol = ? AND gap_start = ?), 0
                    ) + 1, ?)
                """, (
                    gap['symbol'],
                    gap['gap_start'],
                    gap['gap_end'],
                    gap.get('gap_duration_hours', 0),
                    'completed' if backfilled else 'failed',
                    gap['symbol'],
                    gap['gap_start'],
                    datetime.now().isoformat()
                ))

            logger.info(f"Updated gap status for {gap['symbol']}: {'completed' if backfilled else 'failed'}")

        except Exception as e:
            logger.error(f"Failed to update gap status: {e}")

    async def schedule_backfill(self, symbol: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Schedule a comprehensive backfill for missing data"""
        logger.info(f"📅 Scheduling backfill for {symbol or 'all symbols'}")

        try:
            # Detect gaps first
            gaps = self.db_manager.detect_gaps(symbol if symbol else None, 90)  # Look back 90 days

            if not gaps:
                logger.info("No gaps detected, performing full backfill check")

                # If no gaps detected, check for missing data by comparing with expected ranges
                gaps = await self._find_missing_data_ranges(symbol, start_date, end_date)

            # Start backfill process
            result = await self.backfill_gaps(gaps, priority='auto')

            logger.info(f"✅ Backfill scheduling completed: {result}")
            return result

        except Exception as e:
            logger.error(f"❌ Backfill scheduling failed: {e}")
            return {'success': False, 'error': str(e)}

    async def _find_missing_data_ranges(self, symbol: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Find missing data ranges by analyzing existing data"""
        logger.info("🔍 Finding missing data ranges")

        try:
            # Get current data coverage
            db_info = self.db_manager.get_database_info()
            data_coverage = db_info.get('data_coverage', [])

            if symbol:
                data_coverage = [item for item in data_coverage if item[0] == symbol]

            missing_ranges = []

            for coverage_item in data_coverage:
                symbol_name = coverage_item[0]
                min_date = pd.to_datetime(coverage_item[2])
                max_date = pd.to_datetime(coverage_item[3])

                # Check for gaps in the middle of the range
                # This is a simplified version - in practice you'd need more sophisticated analysis
                expected_start = start_date or '2024-01-01'
                expected_end = end_date or datetime.now().isoformat()

                expected_start_dt = pd.to_datetime(expected_start)
                expected_end_dt = pd.to_datetime(expected_end)

                # Check if data starts after expected start
                if min_date > expected_start_dt:
                    missing_ranges.append({
                        'symbol': symbol_name,
                        'gap_start': expected_start_dt.isoformat(),
                        'gap_end': min_date.isoformat(),
                        'gap_duration_hours': (min_date - expected_start_dt).total_seconds() / 3600,
                        'severity': 'high',
                        'type': 'missing_start_data'
                    })

                # Check if data ends before expected end
                if max_date < expected_end_dt:
                    missing_ranges.append({
                        'symbol': symbol_name,
                        'gap_start': max_date.isoformat(),
                        'gap_end': expected_end_dt.isoformat(),
                        'gap_duration_hours': (expected_end_dt - max_date).total_seconds() / 3600,
                        'severity': 'medium',
                        'type': 'missing_recent_data'
                    })

            logger.info(f"Found {len(missing_ranges)} missing data ranges")
            return missing_ranges

        except Exception as e:
            logger.error(f"Error finding missing data ranges: {e}")
            return []