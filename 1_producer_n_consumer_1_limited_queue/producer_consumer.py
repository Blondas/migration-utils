import psycopg2
from queue import Queue
import threading
from typing import List, Any
import time
from dataclasses import dataclass
from contextlib import contextmanager
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Metrics:
    batches_produced: int = 0
    batches_processed: int = 0
    rows_processed: int = 0
    start_time: datetime = None
    
    def log_metrics(self):
        if not self.start_time:
            return
        
        duration = (datetime.now() - self.start_time).total_seconds()
        logger.info(
            f"Processed {self.rows_processed:,} rows in {duration:.2f}s "
            f"({self.rows_processed/duration:.0f} rows/s). "
            f"Batches: produced={self.batches_produced:,}, "
            f"processed={self.batches_processed:,}"
        )

class DataProcessor:
    def __init__(
        self,
        db_conn_string: str,
        batch_size: int = 10000,
        num_consumers: int = 4,
        queue_size: int = 8
    ):
        self.db_conn_string = db_conn_string
        self.batch_size = batch_size
        self.num_consumers = num_consumers
        self.queue = Queue(maxsize=queue_size)
        self.shutdown_event = threading.Event()
        self.metrics = Metrics()
        
    @contextmanager
    def get_db_cursor(self):
        """Context manager for database connections"""
        conn = psycopg2.connect(self.db_conn_string)
        try:
            # Set appropriate cursor options
            conn.set_session(readonly=True)
            cursor = conn.cursor(name='fetch_large_dataset')
            cursor.itersize = self.batch_size
            try:
                yield cursor
            finally:
                cursor.close()
        finally:
            conn.close()

    def producer(self):
        """Produces batches from database"""
        try:
            with self.get_db_cursor() as cursor:
                cursor.execute("""
                    SELECT * 
                    FROM large_table 
                    ORDER BY id  -- Ensuring consistent ordering
                """)
                
                batch = []
                for row in cursor:
                    if self.shutdown_event.is_set():
                        logger.warning("Producer received shutdown signal")
                        break
                        
                    batch.append(row)
                    if len(batch) >= self.batch_size:
                        self.queue.put(batch)
                        self.metrics.batches_produced += 1
                        batch = []
                        
                # Handle last batch
                if batch and not self.shutdown_event.is_set():
                    self.queue.put(batch)
                    self.metrics.batches_produced += 1
                    
        except Exception as e:
            logger.error(f"Producer failed: {e}")
            self.shutdown_event.set()
            raise
        finally:
            # Signal consumers that no more data is coming
            for _ in range(self.num_consumers):
                self.queue.put(None)

    def consumer(self):
        """Processes batches from queue"""
        while not self.shutdown_event.is_set():
            try:
                batch = self.queue.get()
                if batch is None:  # Shutdown signal
                    break
                    
                self._process_batch(batch)
                self.metrics.batches_processed += 1
                self.metrics.rows_processed += len(batch)
                
                # Log progress periodically
                if self.metrics.batches_processed % 100 == 0:
                    self.metrics.log_metrics()
                    
            except Exception as e:
                logger.error(f"Consumer failed: {e}")
                self.shutdown_event.set()
                raise
            finally:
                self.queue.task_done()

    def _process_batch(self, batch: List[Any]):
        """Your actual processing logic here"""
        for row in batch:
            # Your IO operation here
            time.sleep(0.01)  # Simulate IO

    def process_dataset(self):
        """Main processing method"""
        self.metrics.start_time = datetime.now()
        logger.info(f"Starting processing with {self.num_consumers} consumers")
        
        # Start producer
        producer_thread = threading.Thread(target=self.producer)
        producer_thread.start()
        
        # Start consumers
        consumers = []
        for i in range(self.num_consumers):
            consumer = threading.Thread(
                target=self.consumer, 
                name=f'consumer-{i}'
            )
            consumer.start()
            consumers.append(consumer)
        
        # Wait for completion
        producer_thread.join()
        for consumer in consumers:
            consumer.join()
            
        if self.shutdown_event.is_set():
            raise RuntimeError("Processing failed - check logs for details")
            
        self.metrics.log_metrics()

# Usage
if __name__ == "__main__":
    processor = DataProcessor(
        db_conn_string="dbname=your_db user=user password=pass",
        batch_size=10000,
        num_consumers=4,
        queue_size=8
    )
    
    try:
        processor.process_dataset()
    except Exception as e:
        logger.error(f"Processing failed: {e}")
