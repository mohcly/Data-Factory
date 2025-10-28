"""
Parallel Processor for Data Fetching Bot
Handles concurrent data fetching and processing with worker pools
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any, Tuple
import concurrent.futures
import threading
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ProcessingTask:
    """Represents a single data processing task"""
    task_id: str
    symbol: str
    operation: str
    params: Dict = field(default_factory=dict)
    priority: int = 1  # Higher number = higher priority
    created_at: datetime = field(default_factory=datetime.now)
    retries: int = 0
    max_retries: int = 3


@dataclass
class TaskResult:
    """Result of a processing task"""
    task_id: str
    success: bool
    data: Any = None
    error: str = None
    execution_time: float = 0.0
    completed_at: datetime = field(default_factory=datetime.now)


class ParallelProcessor:
    """Manages parallel processing of data fetching tasks"""

    def __init__(self, max_workers: int = 5, max_queue_size: int = 1000):
        """Initialize parallel processor"""
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size

        # Task management
        self.task_queue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.active_tasks = {}
        self.completed_tasks = {}

        # Worker management
        self.workers = []
        self.executor = None
        self.running = False

        # Performance tracking
        self.total_tasks_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0

        # Threading
        self._lock = threading.Lock()

        logger.info(f"Parallel Processor initialized with {max_workers} max workers")

    async def start(self):
        """Start the parallel processor"""
        if self.running:
            logger.warning("Parallel processor already running")
            return

        self.running = True
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        # Start worker tasks
        for i in range(self.max_workers):
            worker_task = asyncio.create_task(self._worker_loop(i))
            self.workers.append(worker_task)

        logger.info(f"✅ Parallel Processor started with {self.max_workers} workers")

    async def stop(self):
        """Stop the parallel processor"""
        if not self.running:
            return

        logger.info("🛑 Stopping Parallel Processor...")
        self.running = False

        # Cancel all workers
        for worker in self.workers:
            if not worker.done():
                worker.cancel()

        # Wait for workers to complete
        await asyncio.gather(*self.workers, return_exceptions=True)

        # Shutdown executor
        if self.executor:
            self.executor.shutdown(wait=True)

        logger.info("✅ Parallel Processor stopped")

    async def submit_task(self,
                         symbol: str,
                         operation: str,
                         params: Dict = None,
                         priority: int = 1) -> str:
        """Submit a task for parallel processing"""
        if not self.running:
            raise RuntimeError("Parallel processor is not running")

        task = ProcessingTask(
            task_id=f"{symbol}_{operation}_{datetime.now().timestamp()}",
            symbol=symbol,
            operation=operation,
            params=params or {},
            priority=priority
        )

        try:
            # Add to priority queue (lower number = higher priority)
            await self.task_queue.put((-priority, task))
            logger.debug(f"📝 Task submitted: {task.task_id}")
            return task.task_id
        except asyncio.QueueFull:
            raise RuntimeError(f"Task queue is full (max: {self.max_queue_size})")

    async def _worker_loop(self, worker_id: int):
        """Main worker loop for processing tasks"""
        logger.debug(f"Worker {worker_id} started")

        while self.running:
            try:
                # Get task from queue with timeout
                try:
                    _, task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                logger.debug(f"Worker {worker_id} processing task: {task.task_id}")

                # Process the task
                result = await self._process_task(task)

                # Store result
                with self._lock:
                    self.completed_tasks[task.task_id] = result
                    self.total_tasks_processed += 1
                    if result.success:
                        self.successful_tasks += 1
                    else:
                        self.failed_tasks += 1

                # Mark task as done
                self.task_queue.task_done()

                logger.debug(f"Worker {worker_id} completed task: {task.task_id}")

            except asyncio.CancelledError:
                logger.debug(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")

        logger.debug(f"Worker {worker_id} stopped")

    async def _process_task(self, task: ProcessingTask) -> TaskResult:
        """Process a single task"""
        start_time = datetime.now()

        try:
            # Execute the task based on operation type
            if task.operation == 'fetch_recent_data':
                data = await self._fetch_recent_data(task.symbol, task.params)
            elif task.operation == 'fetch_historical_chunk':
                data = await self._fetch_historical_chunk(task.symbol, task.params)
            elif task.operation == 'validate_data':
                data = await self._validate_data_task(task.params)
            else:
                raise ValueError(f"Unknown operation: {task.operation}")

            execution_time = (datetime.now() - start_time).total_seconds()

            return TaskResult(
                task_id=task.task_id,
                success=True,
                data=data,
                execution_time=execution_time
            )

        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()

            return TaskResult(
                task_id=task.task_id,
                success=False,
                error=str(e),
                execution_time=execution_time
            )

    async def _fetch_recent_data(self, symbol: str, params: Dict) -> Dict:
        """Fetch recent data for a symbol"""
        # This would integrate with the API manager
        # For now, return a placeholder
        await asyncio.sleep(0.1)  # Simulate API call

        return {
            'symbol': symbol,
            'data_points': 24,
            'start_time': datetime.now() - timedelta(days=1),
            'end_time': datetime.now()
        }

    async def _fetch_historical_chunk(self, symbol: str, params: Dict) -> Dict:
        """Fetch historical data chunk for a symbol"""
        # This would integrate with the backfill manager
        # For now, return a placeholder
        await asyncio.sleep(0.2)  # Simulate longer operation

        return {
            'symbol': symbol,
            'chunk_start': params.get('chunk_start'),
            'chunk_end': params.get('chunk_end'),
            'data_points': 100,
            'backfill_success': True
        }

    async def _validate_data_task(self, params: Dict) -> Dict:
        """Validate data as a separate task"""
        # This would integrate with the data validator
        # For now, return a placeholder
        await asyncio.sleep(0.05)  # Simulate validation

        return {
            'validation_complete': True,
            'quality_score': 0.95,
            'errors': [],
            'warnings': []
        }

    def get_task_status(self, task_id: str) -> Optional[TaskResult]:
        """Get status of a specific task"""
        return self.completed_tasks.get(task_id)

    def get_processing_stats(self) -> Dict:
        """Get processing statistics"""
        active_count = len(self.active_tasks)
        completed_count = len(self.completed_tasks)
        total_processed = self.total_tasks_processed

        return {
            'active_tasks': active_count,
            'completed_tasks': completed_count,
            'total_processed': total_processed,
            'successful_tasks': self.successful_tasks,
            'failed_tasks': self.failed_tasks,
            'success_rate': (self.successful_tasks / total_processed) if total_processed > 0 else 0,
            'queue_size': self.task_queue.qsize() if hasattr(self.task_queue, 'qsize') else 0,
            'running': self.running
        }

    async def wait_for_completion(self, timeout: float = None) -> bool:
        """Wait for all queued tasks to complete"""
        try:
            await asyncio.wait_for(self.task_queue.join(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for task completion after {timeout}s")
            return False

    def clear_completed_tasks(self, older_than_hours: int = 24):
        """Clear old completed tasks to save memory"""
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)

        with self._lock:
            tasks_to_remove = [
                task_id for task_id, result in self.completed_tasks.items()
                if result.completed_at < cutoff_time
            ]

            for task_id in tasks_to_remove:
                del self.completed_tasks[task_id]

            logger.info(f"Cleared {len(tasks_to_remove)} completed tasks older than {older_than_hours} hours")


class ConcurrentDataFetcher:
    """High-level interface for concurrent data fetching operations"""

    def __init__(self, parallel_processor: ParallelProcessor):
        """Initialize concurrent data fetcher"""
        self.processor = parallel_processor

    async def fetch_multiple_symbols(self,
                                   symbols: List[str],
                                   operation: str = 'fetch_recent_data',
                                   **common_params) -> Dict:
        """Fetch data for multiple symbols concurrently"""
        logger.info(f"🚀 Starting concurrent fetch for {len(symbols)} symbols")

        # Submit tasks for all symbols
        task_ids = []
        for symbol in symbols:
            try:
                task_id = await self.processor.submit_task(
                    symbol=symbol,
                    operation=operation,
                    params=common_params,
                    priority=2  # Higher priority for multi-symbol operations
                )
                task_ids.append(task_id)
            except Exception as e:
                logger.error(f"Failed to submit task for {symbol}: {e}")

        # Wait for all tasks to complete
        await self.processor.wait_for_completion(timeout=300)  # 5 minute timeout

        # Collect results
        results = {}
        for task_id in task_ids:
            result = self.processor.get_task_status(task_id)
            if result and result.success:
                # Extract symbol from task_id (format: symbol_operation_timestamp)
                symbol = task_id.split('_')[0]
                results[symbol] = result.data

        success_count = len([r for r in results.values() if r])
        logger.info(f"✅ Concurrent fetch completed: {success_count}/{len(symbols)} successful")

        return {
            'success_count': success_count,
            'total_count': len(symbols),
            'results': results,
            'stats': self.processor.get_processing_stats()
        }

    async def process_backfill_batch(self,
                                   backfill_tasks: List[Dict],
                                   max_concurrent: int = 3) -> Dict:
        """Process multiple backfill tasks concurrently"""
        logger.info(f"📜 Starting concurrent backfill for {len(backfill_tasks)} tasks")

        # Submit backfill tasks with lower priority
        task_ids = []
        for task in backfill_tasks:
            try:
                task_id = await self.processor.submit_task(
                    symbol=task['symbol'],
                    operation='fetch_historical_chunk',
                    params=task.get('params', {}),
                    priority=1  # Lower priority for backfill
                )
                task_ids.append(task_id)
            except Exception as e:
                logger.error(f"Failed to submit backfill task for {task.get('symbol', 'unknown')}: {e}")

        # Wait for completion
        await self.processor.wait_for_completion(timeout=600)  # 10 minute timeout for backfills

        # Collect results
        results = {}
        for task_id in task_ids:
            result = self.processor.get_task_status(task_id)
            if result and result.success:
                symbol = task_id.split('_')[0]
                results[symbol] = result.data

        success_count = len(results)
        logger.info(f"✅ Concurrent backfill completed: {success_count}/{len(backfill_tasks)} successful")

        return {
            'success_count': success_count,
            'total_count': len(backfill_tasks),
            'results': results,
            'stats': self.processor.get_processing_stats()
        }