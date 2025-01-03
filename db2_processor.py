import json
import signal
import statistics
from logging import Logger
from pathlib import Path

import ibm_db_dbi
import threading
from queue import Queue, Empty
from typing import List, Optional, Tuple, Iterator, Set, NamedTuple, Callable, Dict, Any
from contextlib import contextmanager
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
import re
import subprocess
import yaml
import argparse
import psutil
import os


# Create a module-level logger that will be replaced in main()
logger = logging.getLogger(__name__)

def setup_logging(label: str) -> Logger:
    """Configure logging with label-specific log file"""
    # Create handler instances
    stream_handler = logging.StreamHandler()
    log_filename = f'processing-{label}.log' if label else 'processing.log'
    file_handler = logging.FileHandler(filename=log_filename)

    # Create formatter and set it for the handlers
    formatter = logging.Formatter(
        f'%(asctime)s.%(msecs)03d | %(levelname)-8s | {label:<10} | %(threadName)-12s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Configure the logging
    logging.basicConfig(level=logging.INFO, handlers=[stream_handler, file_handler])
    return logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    # Database
    database: str

    # Producer Consumer setup
    read_batch_size: int
    num_consumers: int
    consumers_queue_size: int

    # DB updater setup
    update_queue_size: int
    update_status: bool

    # Arsadmin setup
    command_max_objects: int
    dir_max_elems: int
    user: str
    password: Optional[str]
    od_inst: str
    base_dir: str

    # Monitoring
    metrics_interval_seconds: int
    minimum_disk_space_percentage: int
    disk_interval_seconds: int
    runtime_statistics_interval: int
    timeout_seconds: Optional[int]


class ProcessingStatus(Enum):
    NOTSTARTED = 'notstarted'
    STARTED = 'started'
    COMPLETED = 'completed'
    FAILED = 'failed'


class DBRow(NamedTuple):
    id: int # ID
    tape_id: str # ODSLOC
    create_dt: datetime # ODCREATES
    agid_name: str # AGID_NAME
    agname: str # AGNAME
    object_id: str # LOADID
    pri_nid: int # PRINID
    status: str # STATUS
    processed_dt: Optional[datetime] # DTSTAMP


@dataclass(frozen=True)
class ObjectRecord:
    """Map object id to db record id"""
    db_record_id: int
    object_id: str


@dataclass(frozen=True)
class Command:
    """Groups objects from the same tape, od_inst, and pri_nid"""
    od_inst: str
    user: str
    password: Optional[str]
    agname: str
    pri_nid: int
    dest_subdir: str
    object_records: List[ObjectRecord]


@dataclass
class CommandResult:
    successful_ids: Set[int]
    failed_ids: Set[int]  # object_name -> error message


@dataclass
class StatusUpdate:
    ids: Set[int]
    status: ProcessingStatus


@dataclass
class ProcessingStats:
    processed_commands: int = 0
    processed_objects: int = 0
    last_log_time: float = field(default_factory=time.time)


class MetricsMonitor:
    def __init__(
            self,
            log_interval: float
    ) -> None:
        self._queue: Optional[Queue] = None
        self._update_queue: Optional[Queue] = None
        self.log_interval = log_interval
        self.stats = ProcessingStats()
        self._shutdown_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the metrics monitoring thread"""
        logger.info("Starting metrics monitoring thread...")
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name='metrics_monitor'
        )
        self._monitor_thread.daemon = True  # Thread will exit when main thread exits
        self._monitor_thread.start()

    def stop(self) -> None:
        """Stop the metrics monitoring thread"""
        self._shutdown_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)

    def increment_processed(self, command_object_count: int) -> None:
        """Increment the processed objects counter"""
        self.stats.processed_objects += command_object_count
        self.stats.processed_commands += 1

    def set_queues(self, queue: Queue, update_queue: Queue) -> None:
        """Set the queue to monitor"""
        self._queue = queue
        self._update_queue = update_queue

    def _monitor_loop(self) -> None:
        """Main monitoring loop that periodically logs metrics"""
        while not self._shutdown_event.is_set():
            try:
                current_time = time.time()
                if current_time - self.stats.last_log_time >= self.log_interval:
                    self._log_metrics()
                    self.stats.last_log_time = current_time

                time.sleep(1.0)  # Avoid tight loop
            except Exception as e:
                logger.error(f"Error in metrics monitor: {e}")
                time.sleep(5.0)  # Back off on error

    def _log_metrics(self) -> None:
        """Log current metrics"""
        # Log formatted metrics
        log_entries = [
            "-" * 80,
            "Current Processing Metrics:",
            f"Queue size: {self._queue.qsize()}/{self._queue.maxsize}",
            f"Update queue size: {self._update_queue.qsize()}/{self._update_queue.maxsize}",
            f"Commands processed: {self.stats.processed_commands:,}",
            f"Total objects processed: {self.stats.processed_objects:,}",
            "-" * 80
        ]

        for entry in log_entries:
            logger.info(entry)

        # Log metrics as JSON
        metrics_dict = {
            "queue_size": self._queue.qsize(),
            "queue_maxsize": self._queue.maxsize,
            "update_queue_size": self._update_queue.qsize(),
            "update_queue_maxsize": self._update_queue.maxsize,
            "commands_processed": self.stats.processed_commands,
            "total_objects_processed": self.stats.processed_objects,
            "queue_utilization_percentage": round((self._queue.qsize() / self._queue.maxsize) * 100, 2),
            "update_queue_utilization_percentage": round(
                (self._update_queue.qsize() / self._update_queue.maxsize) * 100, 2)
        }

        try:
            logger.info(f"Metrics JSON: {json.dumps(metrics_dict)}")
        except Exception as e:
            logger.error(f"Failed to serialize metrics to JSON: {str(e)}")


class DiskSpaceMonitor:
    def __init__(
        self,
        path: str,
        minimum_disk_space_percentage: int,
        interval_seconds: int,
        terminal_operation: Callable[[], None]
    ) -> None:
        self.path: str = path
        self.minimum_disk_space_percentage: int = minimum_disk_space_percentage
        self.interval_seconds: int = interval_seconds

        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event: threading.Event = threading.Event()
        self.terminal_operation: Callable[[], None] = terminal_operation

    def start(self) -> None:
        """Start disk monitoring in a separate thread"""
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name='disk_monitor'
        )
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        logging.info("Disk space monitoring started")

    def stop(self) -> None:
        """Stop disk monitoring"""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)

    def _get_disk_usage(self) -> float:
        """Returns free disk space percentage"""
        usage = psutil.disk_usage(self.path)
        return usage.free / usage.total * 100

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                free_space = self._get_disk_usage()
                logger.info(f"Free disk space: {free_space:.2f}%")
                if free_space < self.minimum_disk_space_percentage:
                    logging.error(
                        f"Free disk space {free_space:.2f}% below threshold "
                        f"{self.minimum_disk_space_percentage}%. "
                        f"Running terminal operation..."
                    )

                    # do the terminal operation before killing
                    self.terminal_operation()

                    logging.info("Killing process with SIGTERM...")
                    # Kill the entire process group
                    os.kill(os.getpid(), signal.SIGTERM)

                    # If SIGTERM doesn't work, use SIGKILL after a short delay
                    time.sleep(2)
                    logging.info("Killing process with SIGKILL...")
                    os.kill(os.getpid(), signal.SIGKILL)

                threading.Event().wait(self.interval_seconds)

            except Exception as e:
                logging.error(f"Disk monitoring error: {e}")
                threading.Event().wait(5.0)  # Back off on error


@dataclass(frozen=True)
class RuntimeStatistics:
    runtime_seconds: float
    total_files: int
    total_size_bytes: int
    median_size_bytes: float
    min_size_bytes: int
    max_size_bytes: int

    def get_processing_rate(self) -> float:
        """Calculate files processed per second"""
        if self.runtime_seconds == 0:
            return 0.0
        return self.total_files / self.runtime_seconds

    def get_throughput(self) -> float:
        """Calculate bytes processed per second"""
        if self.runtime_seconds == 0:
            return 0.0
        return self.total_size_bytes / self.runtime_seconds

    def average_file_size(self) -> float:
        """Calculate average file size"""
        if self.total_files == 0:
            return 0.0
        return self.total_size_bytes / self.total_files


class RuntimeStatisticsCalculator:
        """Handles calculation and formatting of processing metrics"""

        def __init__(self, base_dir: str, interval_seconds: int) -> None:
            self.base_dir: str = base_dir
            self.start_time: float = time.time()
            self.interval_seconds: int = interval_seconds
            self._shutdown_event: threading.Event = threading.Event()
            self._monitor_thread: Optional[threading.Thread] = None

        def start(self) -> None:
            """Start the monitoring thread"""
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                name='runtime_stats_monitor'
            )
            self._monitor_thread.daemon = True
            self._monitor_thread.start()
            logger.info("Runtime statistics monitoring started")

        def stop(self) -> None:
            metrics: RuntimeStatistics = self._calculate_metrics()
            self._log_metrics(metrics)

            """Stop the monitoring thread"""
            self._shutdown_event.set()
            if self._monitor_thread:
                self._monitor_thread.join(timeout=5.0)
            logger.info("Runtime statistics monitoring stopped")

        def _monitor_loop(self) -> None:
            """Main monitoring loop"""
            while not self._shutdown_event.is_set():
                try:
                    metrics: RuntimeStatistics = self._calculate_metrics()
                    self._log_metrics(metrics)

                    # Sleep until next interval
                    time.sleep(self.interval_seconds)

                except Exception as e:
                    logger.error(f"Error in runtime statistics monitor: {e}")
                    time.sleep(5.0)  # Back off on error

        def _calculate_metrics(self) -> RuntimeStatistics:
            """Calculate all metrics for files in directory tree"""
            path = Path(self.base_dir)
            if not path.exists():
                raise ValueError(f"Directory {self.base_dir} does not exist")

            file_sizes: list[int] = []

            # Walk through all subdirectories
            for file_path in path.rglob('*'):
                if file_path.is_file():
                    file_sizes.append(file_path.stat().st_size)

            if not file_sizes:
                return RuntimeStatistics(
                    runtime_seconds=0,
                    total_files=0,
                    total_size_bytes=0,
                    median_size_bytes=0,
                    min_size_bytes=0,
                    max_size_bytes=0
                )

            runtime = time.time() - self.start_time

            return RuntimeStatistics(
                runtime_seconds=runtime,
                total_files=len(file_sizes),
                total_size_bytes=sum(file_sizes),
                median_size_bytes=statistics.median(file_sizes),
                min_size_bytes=min(file_sizes),
                max_size_bytes=max(file_sizes)
            )

        def calculate_and_log_metrics(self) -> None:
            self._log_metrics(self._calculate_metrics())

        @staticmethod
        def format_size(size_bytes: int) -> str:
            """Format byte size into human-readable format"""
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if size_bytes < 1024.0:
                    return f"{size_bytes:.2f} {unit}"
                size_bytes /= 1024.0
            return f"{size_bytes:.2f} PB"

        @staticmethod
        def format_runtime(seconds: float) -> str:
            """Format runtime into human-readable format"""
            delta = timedelta(seconds=seconds)
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            seconds = delta.seconds % 60

            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        def _log_metrics(self, metrics: RuntimeStatistics) -> None:
            # log formatted metrics
            log_entries = [
                "-" * 80,
                "Processing completed. Final metrics:",
                f"Total runtime: {self.format_runtime(metrics.runtime_seconds)} ({metrics.runtime_seconds:.2f} seconds)",
                f"Total files processed: {metrics.total_files:,}",
                f"Total size: {self.format_size(metrics.total_size_bytes)} ({metrics.total_size_bytes:,} bytes)",
                f"Average file size: {self.format_size(int(metrics.average_file_size()))} ({int(metrics.average_file_size()):,} bytes)",
                f"Median file size: {self.format_size(int(metrics.median_size_bytes))} ({int(metrics.median_size_bytes):,} bytes)",
                f"Smallest file: {self.format_size(metrics.min_size_bytes)} ({metrics.min_size_bytes:,} bytes)",
                f"Largest file: {self.format_size(metrics.max_size_bytes)} ({metrics.max_size_bytes:,} bytes)",
                f"Processing rate: {metrics.get_processing_rate() * 60:.2f} files/minute",
                f"Throughput: {self.format_size(int(metrics.get_throughput() * 60))}/minute",
                "-" * 80
            ]
            for entry in log_entries:
                logger.info(entry)

            # log metrics as json
            metrics_dict: Dict[str, Any] = {
                "runtime_seconds": round(metrics.runtime_seconds, 2),
                "total_files": metrics.total_files,
                "total_size_bytes": metrics.total_size_bytes,
                "average_file_size_bytes": int(metrics.average_file_size()),
                "median_size_bytes": int(metrics.median_size_bytes),
                "min_size_bytes": metrics.min_size_bytes,
                "max_size_bytes": metrics.max_size_bytes,
                "processing_rate_files_per_minute": round(metrics.get_processing_rate() * 60, 2),
                "throughput_bytes_per_minute": int(metrics.get_throughput()) * 60
            }
            try:
                logger.info(f"Metrics JSON: {json.dumps(metrics_dict)}")
            except Exception as e:
                logger.error(f"Failed to serialize metrics to JSON: {str(e)}")


class CommandBatchBuilder:
    """
    Builds TapeCommandBatch objects from database rows belonging to the same tape_id.
    """
    def __init__(
        self,
        command_max_objects: int,
        dir_max_elems: int,
        user: str,
        password: Optional[str],
        od_inst: str,
        base_dir: str
    ) -> None:
        if command_max_objects >= dir_max_elems:
            raise ValueError(
                "max_objects must be less than dir_max_elems"
            )
        self.max_objects = command_max_objects
        self.dir_max_elems = dir_max_elems
        self.user = user
        self.password = password
        self.od_inst = od_inst
        self._dir_prefix = base_dir
        self._current_batch_no: int = 0

    def _get_subfolder_path(self, agid_name :str):
        command_subdir = self._current_batch_no % self.dir_max_elems
        if command_subdir == 0:
            command_subdir = self.dir_max_elems
        return os.path.abspath(os.path.join(
            self._dir_prefix,
            agid_name,
            "batch_" + str((self._current_batch_no // self.dir_max_elems) + 1),
            "command_" + str(command_subdir)
        ))

    def build_tape_commands(
        self,
        rows: List[DBRow]
    ) -> List[Command]:
        logger.debug(f"build_tape_commands, Building tape commands for {len(rows)} rows")
        """
        Creates TapeCommandBatch objects from rows of a single tape_id.
        Expects rows to be sorted by: agname, prinid, odcreats
        """
        if not rows:
            return []

        # Verify all rows have the same tape_id
        tape_id = rows[0].tape_id
        if not all(r.tape_id == tape_id for r in rows):
            raise ValueError("All rows must have the same tape_id")

        command_batches: List[Command] = []
        current_object_records: List[ObjectRecord] = []
        current_pri_nid: Optional[int] = None
        current_agname: Optional[str] = None
        current_agid_name: Optional[str] = None

        for row in rows:
            # Start new batch if pri_nid changes or max objects reached
            if (current_pri_nid != row.pri_nid or
                current_agname != row.agname or
                len(current_object_records) >= self.max_objects):
                if current_object_records:
                    self._current_batch_no += 1

                    command_batches.append(
                        Command(
                            od_inst=self.od_inst,
                            user=self.user,
                            password=self.password,
                            agname=current_agname,
                            pri_nid=current_pri_nid,
                            dest_subdir=self._get_subfolder_path(current_agid_name),
                            object_records=current_object_records
                        )
                    )
                current_object_records = []
                current_pri_nid = row.pri_nid
                current_agname = row.agname
                current_agid_name = row.agid_name

            current_object_records.append(ObjectRecord(row.id, row.object_id))

        # Handle last group
        if current_object_records:
            self._current_batch_no += 1
            command_batches.append(
                Command(
                    od_inst=self.od_inst,
                    user=self.user,
                    password=self.password,
                    agname=current_agname,
                    pri_nid=current_pri_nid,
                    dest_subdir=self._get_subfolder_path(current_agid_name),
                    object_records=current_object_records
                )
            )

        return command_batches

    def simple_build_commands(
        self,
        rows: List[DBRow]
    ) -> List[Command]:
        logger.debug(f"build_tape_commands, Building tape commands for {len(rows)} rows")
        """
        Creates List[Command] without further constraints from all rows.
        Expects rows to be sorted by: agname, odsloc, odcreats
        """
        if not rows:
            return []

        command_batches: List[Command] = []
        current_object_records: List[ObjectRecord] = []
        current_pri_nid: Optional[int] = None
        current_agname: Optional[str] = None
        current_agid_name: Optional[str] = None

        for row in rows:
            # Start new batch if pri_nid changes or max objects reached
            if (current_pri_nid != row.pri_nid or
                current_agname != row.agname or
                len(current_object_records) >= self.max_objects):
                if current_object_records:
                    self._current_batch_no += 1

                    command_batches.append(
                        Command(
                            od_inst=self.od_inst,
                            user=self.user,
                            password=self.password,
                            agname=current_agname,
                            pri_nid=current_pri_nid,
                            dest_subdir=self._get_subfolder_path(current_agid_name),
                            object_records=current_object_records
                        )
                    )
                current_object_records = []
                current_pri_nid = row.pri_nid
                current_agname = row.agname
                current_agid_name = row.agid_name

            current_object_records.append(ObjectRecord(row.id, row.object_id))

        # Handle last group
        if current_object_records:
            self._current_batch_no += 1
            command_batches.append(
                Command(
                    od_inst=self.od_inst,
                    user=self.user,
                    password=self.password,
                    agname=current_agname,
                    pri_nid=current_pri_nid,
                    dest_subdir=self._get_subfolder_path(current_agid_name),
                    object_records=current_object_records
                )
            )

        return command_batches


class DB2Connection:
    def __init__(self, database: str, for_updates: bool = False) -> None:
        self.database: str = database
        self.user: str = ''
        self.password: str = ''
        self.for_updates: bool = for_updates

    @contextmanager
    def get_cursor(self) -> Iterator[ibm_db_dbi.Cursor]:
        try:
            conn: ibm_db_dbi.Connection = ibm_db_dbi.connect(self.database, self.user, self.password)
        except Exception as e:
            logger.error(f"Failed to establish database connection: {str(e)}")
            raise

        try:
            logger.debug("Attempting to create cursor")
            cursor: ibm_db_dbi.Cursor = conn.cursor()
            logger.debug("Cursor created successfully")

            if self.for_updates:
                logger.debug("Setting isolation level CS for updates")
                try:
                    cursor.execute("SET CURRENT ISOLATION = CS")
                    cursor.execute("SET CURRENT LOCK TIMEOUT = 30")
                    logger.debug("Update cursor settings applied successfully")
                except Exception as e:
                    logger.error(f"Failed to set update cursor settings: {str(e)}")
                    raise
            else:
                logger.debug("Setting read-only cursor settings")
                try:
                    cursor.execute("SET CURRENT ISOLATION = UR")
                    cursor.execute("SET CURRENT QUERY OPTIMIZATION = 5")
                    cursor.execute("SET CURRENT DEGREE = 'ANY'")
                    logger.debug("Read-only cursor settings applied successfully")
                except Exception as e:
                    logger.error(f"Failed to set read-only cursor settings: {str(e)}")
                    raise

            yield cursor

            if self.for_updates:
                logger.debug("Committing transaction")
                conn.commit()
                logger.debug("Transaction committed successfully")

        except Exception as e:
            if self.for_updates:
                logger.error(f"Error during cursor operation, rolling back: {str(e)}")
                conn.rollback()
            else:
                logger.error(f"Error during cursor operation: {str(e)}")
            raise
        finally:
            logger.debug("Closing database connection")
            try:
                conn.close()
                logger.debug("Database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")


class StatusUpdateManager:
    def __init__(
            self,
            db: DB2Connection,
            table_name: str,
            queue_size: int,
            update_status: bool
    ) -> None:
        self.db = db
        self.table_name = table_name
        self.queue: Queue[Optional[StatusUpdate]] = Queue(queue_size)
        self.shutdown_event: threading.Event = threading.Event()
        self.update_thread: Optional[threading.Thread] = None
        self.update_status: bool = update_status

    def start(self) -> None:
        self.update_thread = threading.Thread(
            target=self._update_status_worker,
            name='status_updater'
        )
        self.update_thread.start()
        logger.info("Status update manager started")

    def stop(self) -> None:
        self.shutdown_event.set()
        self.queue.put(None)
        if self.update_thread:
            self.update_thread.join()
        logger.info("Status update manager stopped")

    def queue_update(self, status_update: StatusUpdate) -> None:
        if self.queue.full():
            logger.warning("Update-queue is full, may be blocking consumers.")
        if self.update_status:
            self.queue.put(status_update)
        else:
            logger.debug("--update_status=False, skipping status update")

    def _update_status_worker(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                update: Optional[StatusUpdate] = self.queue.get(timeout=1.0)
                if update is None:
                    break

                self._process_single_update(update)

            except Empty:
                continue

            except Exception as e:
                logger.error(f"Status update worker failed: {e}")
                time.sleep(1)

    def _process_single_update(self, update: StatusUpdate) -> None:
        if not self.update_status:
            logger.debug("_process_single_update: Not updating status in db")
            return

        try:
            with self.db.get_cursor() as cursor:
                id_values: str = ",".join(str(idx) for idx in update.ids)
                sql = f"""
                        UPDATE {self.table_name}
                        SET STATUS = ?, 
                        DTSTAMP = CURRENT TIMESTAMP
                        WHERE ID IN ({id_values})
                        """
                cursor.execute(sql, (update.status.value,))

        except Exception as e:
            logger.error(f"Status update failed for ids {update.ids}, error: {e}", exc_info=True)
            raise


class CommandProcessor:
    @staticmethod
    def _ensure_directory_exists(subdir: str) -> None:
        os.makedirs(subdir, exist_ok=True)

    def _execute_command(self, cmd: List[str]) -> Tuple[int, str, str]:
        """Executes command and returns return_code, stdout, stderr"""
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        return process.returncode, process.stdout, process.stderr

    def process_command(self, command: Command) -> CommandResult:
        """
        Processes a single command, handling errors and retries
        based on specific error conditions.
        """
        remaining_object_records: list[ObjectRecord] = command.object_records.copy()
        dictionary: dict[str, int] = {obj.object_id: obj.db_record_id for obj in remaining_object_records}
        successful_ids: Set[int] = set()
        failed_ids: Set[int] = set()

        self._ensure_directory_exists(command.dest_subdir)

        try:
            while remaining_object_records:
                # Build and execute command
                cmd = [
                    "arsadmin", "retrieve",
                    "-I", command.od_inst,
                    '-u', command.user,
                    *(['-p', command.password] if command.password else []),
                    '-g', command.agname,
                    "-n", f'{command.pri_nid}-0',
                    "-d", command.dest_subdir,
                ]
                cmd.extend(object_record.object_id for object_record in remaining_object_records)

                return_code, stdout, stderr = self._execute_command(cmd)

                if return_code != 0:
                    if "ARS1159E Unable to retrieve the object" in stderr:
                        match = re.search(
                            r"Unable to retrieve the object >(\S+)<",
                            stderr
                        )
                        if match:
                            failing_object_id: str = match.group(1)
                            logger.error(
                                f"code: {return_code}, document: {failing_object_id}, "
                                f"message: Unable to retrieve document, "
                                f"skipping current document and re-executing command"
                            )

                            failed_ids.add(dictionary[failing_object_id])

                            # Find index of failing object and continue with remaining ones
                            for i, object_record in enumerate(remaining_object_records):
                                successful_ids.add(object_record.db_record_id)
                                if object_record.object_id == failing_object_id:
                                    remaining_object_records = remaining_object_records[i + 1:]
                                    break

                            continue

                    elif "ARS1168E Unable to determine Storage Node" in stderr:
                        error_msg = f"Unable to determine Storage Node ({command.pri_nid})"
                        logger.error(
                            f"code: {return_code}, message: {error_msg}, "
                            f"skipping remaining documents in this command"
                        )
                        for object_record in remaining_object_records:
                            failed_ids.add(object_record.db_record_id)
                        break

                    elif "ARS1110E The application group" in stderr:
                        error_msg = "The Application Group (or permission) doesn't exist"
                        logger.error(
                            f"code: {return_code}, message: {error_msg}, "
                            f"skipping remaining documents in this command"
                        )
                        for object_record in remaining_object_records:
                            failed_ids.add(object_record.db_record_id)
                        break

                    else:
                        logger.error(
                            f"code: {return_code}, message: {stderr}, "
                            f"skipping remaining documents in this command"
                        )
                        for object_record in remaining_object_records:
                            failed_ids.add(object_record.db_record_id)
                        break

                else:
                    # Command successful - mark all remaining objects as successful
                    for object_record in remaining_object_records:
                        successful_ids.add(object_record.db_record_id)
                    break

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            for object_record in remaining_object_records:
                failed_ids.add(object_record.db_record_id)

        return CommandResult(
            successful_ids=successful_ids,
            failed_ids=failed_ids
        )


class DataProcessor:
    def __init__(
            self,
            read_db: DB2Connection,
            status_update_manager: StatusUpdateManager,
            table_name: str,
            command_batch_builder: CommandBatchBuilder,
            command_processor: CommandProcessor,
            metrics_monitor: MetricsMonitor,
            disk_space_monitor: DiskSpaceMonitor,
            runtime_stats_calculator: RuntimeStatisticsCalculator,
            db_read_batch_size: int,
            num_consumers: int,
            consumers_queue_size: int,
            timeout_seconds: int
    ) -> None:
        self.read_db = read_db
        self.status_update_manager = status_update_manager
        self.table_name = table_name
        self.command_batch_builder = command_batch_builder
        self.command_processor = command_processor
        self.queue: Queue[Optional[List[Command]]] = Queue(maxsize=consumers_queue_size)

        self.metrics_monitor = metrics_monitor
        self.metrics_monitor.set_queues(self.queue, self.status_update_manager.queue)

        self.disk_space_monitor: DiskSpaceMonitor = disk_space_monitor
        self.runtime_statistics_calculator: RuntimeStatisticsCalculator = runtime_stats_calculator

        self.db_read_batch_size = db_read_batch_size
        self.num_consumers = num_consumers
        self.shutdown_event: threading.Event = threading.Event()

        self.timeout_seconds = timeout_seconds
        self.start_time: float = time.time()

    def _check_timeout(self) -> None:
        if self.timeout_seconds and time.time() - self.start_time > self.timeout_seconds:
            logger.error(
                f"Timeout of {self.timeout_seconds} seconds reached, "
                f"Initiating shutdown..."
            )
            self.shutdown_event.set()

    def _fetch_by_tape(self):
        def produce_by_tape(rows: List[DBRow]) -> None:
            if not rows:
                return

            # Create tape commands for the group
            tape_commands: List[Command] = self.command_batch_builder.build_tape_commands(rows)

            # Update status for all objects
            status_update = StatusUpdate(
                ids={
                    obj.db_record_id
                    for cmd in tape_commands
                    for obj in cmd.object_records
                },
                status=ProcessingStatus.STARTED
            )
            self.status_update_manager.queue_update(status_update)

            if self.queue.full():
                logger.warning("Consumer-queue is full, may be blocking producers")

            # Queue the tape commands
            self.queue.put(tape_commands)

        try:
            with self.read_db.get_cursor() as cursor:
                query: str = f"""
                    SELECT 
                        ID,
                        ODSLOC,
                        ODCREATS,
                        AGID_NAME_SRC,
                        AGNAME,
                        LOADID,
                        PRINID,
                        STATUS,
                        DTSTAMP
                    FROM 
                        {self.table_name}
                    WHERE 
                        STATUS = '{ProcessingStatus.NOTSTARTED.value}'
                    ORDER BY 
                        ODSLOC,
                        AGNAME,
                        PRINID,
                        ODCREATS
                    --#SET ISOLATION = UR
                    OPTIMIZE FOR {self.db_read_batch_size} ROWS
                """

                cursor.execute(query)

                buffer: List[DBRow] = []
                current_tape_id: Optional[str] = None

                while True:
                    self._check_timeout()
                    if self.shutdown_event.is_set():
                        break

                    logger.debug("producer, before rows fetched")
                    rows = cursor.fetchmany(self.db_read_batch_size)
                    logger.debug("producer, rows fetched")
                    if not rows:
                        # Process any remaining buffered rows
                        if buffer:
                            produce_by_tape(buffer)
                        break

                    db_rows: list[DBRow] = [DBRow(*row) for row in rows]

                    for row in db_rows:
                        if current_tape_id is None:
                            current_tape_id = row.tape_id

                        if row.tape_id != current_tape_id:
                            # Process complete tape group
                            produce_by_tape(buffer)
                            buffer = [row]
                            current_tape_id = row.tape_id
                        else:
                            buffer.append(row)

                    # If we've processed all rows but still have data in buffer,
                    # wait for next batch as this tape_id group might continue

        except Exception as e:
            logger.error(f"Producer failed: {e}")
            self.shutdown_event.set()
            raise
        finally:
            for _ in range(self.num_consumers):
                self.queue.put(None)

    def _fetch_by_agname(self):
        def simple_produce(rows: List[DBRow]) -> None:
            if not rows:
                return

            # Create commands
            commands: List[Command] = self.command_batch_builder.simple_build_commands(rows)

            # Update status for all objects
            status_update = StatusUpdate(
                ids={
                    obj.db_record_id
                    for cmd in commands
                    for obj in cmd.object_records
                },
                status=ProcessingStatus.STARTED
            )
            self.status_update_manager.queue_update(status_update)

            # Queue the tape commands, only one command in list
            for command in commands:
                if self.queue.full():
                    logger.warning("Consumer-queue is full, may be blocking producers")
                self.queue.put([command])

        try:
            with self.read_db.get_cursor() as cursor:
                query: str = f"""
                    SELECT 
                        ID,
                        ODSLOC,
                        ODCREATS,
                        AGID_NAME_SRC,
                        AGNAME,
                        LOADID,
                        PRINID,
                        STATUS,
                        DTSTAMP
                    FROM 
                        {self.table_name}
                    WHERE 
                        STATUS = '{ProcessingStatus.NOTSTARTED.value}' AND AGNAME != ''
                    ORDER BY 
                        AGNAME,
                        ODSLOC,
                        ODCREATS
                    --#SET ISOLATION = UR
                    OPTIMIZE FOR {self.db_read_batch_size} ROWS
                """

                cursor.execute(query)

                while True:
                    self._check_timeout()
                    if self.shutdown_event.is_set():
                        break

                    rows = cursor.fetchmany(self.db_read_batch_size)
                    if not rows:
                        break

                    db_rows: list[DBRow] = [DBRow(*row) for row in rows]
                    simple_produce(db_rows)

        except Exception as e:
            logger.error(f"Producer failed: {e}")
            self.shutdown_event.set()
            raise
        finally:
            for _ in range(self.num_consumers):
                self.queue.put(None)

    def producer(self) -> None:
        logger.debug("producer thread started")
        self._fetch_by_tape()
        # self._fetch_by_agname()

    def consumer(self) -> None:
        logger.info("consumer started")
        while not self.shutdown_event.is_set():
            try:
                self._check_timeout()
                if self.queue.empty():
                    logger.warning("Consumer-queue is empty, consumer may be idle")

                tape_commands: Optional[List[Command]] = self.queue.get()
                if tape_commands is None:
                    break

                for command in tape_commands:
                    try:
                        command_result: CommandResult = self.command_processor.process_command(command)
                        self.metrics_monitor.increment_processed(len(command.object_records))

                        # Update successful objects
                        if command_result.successful_ids:
                            self.status_update_manager.queue_update(
                                StatusUpdate(
                                    ids=command_result.successful_ids,
                                    status=ProcessingStatus.COMPLETED
                                )
                            )

                        if command_result.failed_ids:
                            self.status_update_manager.queue_update(
                                StatusUpdate(
                                    ids=command_result.failed_ids,
                                    status=ProcessingStatus.FAILED
                                )
                            )

                    except Exception as e:
                        logger.error(f"Failed to process command: {str(e)}")
                        # Mark all objects as failed
                        failed_objects = {
                            obj.db_record_id for obj in command.object_records
                        }
                        self.status_update_manager.queue_update(
                            StatusUpdate(
                                ids=failed_objects,
                                status=ProcessingStatus.FAILED
                            )
                        )

            except Exception as e:
                logger.error(f"Consumer error: {str(e)}")
                if not self.shutdown_event.is_set():
                    time.sleep(1)  # Prevent tight error loop
            finally:
                self.queue.task_done()

    def run(self):
        # Start monitoring daemons, no needs to kill tem explicitly
        self.metrics_monitor.start()
        self.disk_space_monitor.start()
        self.runtime_statistics_calculator.start()

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

        try:
            producer_thread.join()
            for consumer in consumers:
                consumer.join()

            if self.shutdown_event.is_set():
                raise RuntimeError("Processing failed - check logs for details")

        finally:
            self.metrics_monitor.stop()
            self.disk_space_monitor.stop()
            self.runtime_statistics_calculator.stop()


def load_config(config_path: Optional[str] = None) -> Config:
    """Load config from yaml file"""
    if config_path is None:
        config_path = str(Path(__file__).parent / 'config.yaml')

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    return Config(
        # Database
        database=yaml_config['database']['database'],

        # Consumer
        read_batch_size=yaml_config['producer_consumer']['read_batch_size'],
        num_consumers=yaml_config['producer_consumer']['num_consumers'],
        consumers_queue_size=yaml_config['producer_consumer']['consumers_queue_size'],

        # Updater
        update_queue_size=yaml_config['db_updater']['update_queue_size'],
        update_status=yaml_config['db_updater']['update_status'],

        # Arsadmin
        command_max_objects=yaml_config['arsadmin']['command_max_objects'],
        dir_max_elems=yaml_config['arsadmin']['dir_max_elems'],
        user=yaml_config['arsadmin']['user'],
        password=yaml_config['arsadmin'].get('password'),  # Optional
        od_inst=yaml_config['arsadmin']['od_inst'],
        base_dir=yaml_config['arsadmin']['base_dir'],

        # Monitoring
        metrics_interval_seconds=yaml_config['monitoring']['metrics_interval_seconds'],
        minimum_disk_space_percentage=yaml_config['monitoring']['minimum_disk_space_percentage'],
        disk_interval_seconds=yaml_config['monitoring']['disk_interval_seconds'],
        runtime_statistics_interval=yaml_config['monitoring']['runtime_statistics_interval'],
        timeout_seconds=yaml_config['monitoring']['timeout_seconds'],
    )

def main() -> None:
    parser = argparse.ArgumentParser(description='Arsadmin Retrieve Command Executor')
    parser.add_argument('--table_name', help='Table name to drive payload migration', required=True)
    parser.add_argument('--label', help='Optional label to add to the base directory', default='')
    args = parser.parse_args()

    # Setup logging with label
    global logger
    logger = setup_logging(args.label)

    config: Config = load_config()
    logger.info(f"Deleting {config.base_dir}")

    read_db: DB2Connection = DB2Connection(config.database, for_updates=False)
    update_db: DB2Connection = DB2Connection(config.database, for_updates=True)

    # create base_dir:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    label_prefix = f"{args.label}-" if args.label else ""
    base_dir = f'{config.base_dir}/{label_prefix}{current_time}'

    runtime_statistics_calculator = RuntimeStatisticsCalculator(base_dir, config.runtime_statistics_interval)

    tape_batch_builder: CommandBatchBuilder = CommandBatchBuilder(
        command_max_objects = config.command_max_objects,
        dir_max_elems = config.dir_max_elems,
        user = config.user,
        password = config.password,
        od_inst = config.od_inst,
        base_dir= base_dir,
    )
    status_update_manager: StatusUpdateManager = StatusUpdateManager(
        db = update_db,
        table_name= args.table_name,
        queue_size= config.update_queue_size,
        update_status = config.update_status
    )
    command_processor = CommandProcessor()
    disk_space_monitor=DiskSpaceMonitor(
        path=base_dir,
        minimum_disk_space_percentage=config.minimum_disk_space_percentage,
        interval_seconds=config.disk_interval_seconds,
        terminal_operation=runtime_statistics_calculator.calculate_and_log_metrics
    )
    metrics_monitor=MetricsMonitor(log_interval=config.metrics_interval_seconds)
    processor = DataProcessor(
        read_db = read_db,
        status_update_manager = status_update_manager,
        table_name = args.table_name,
        command_batch_builder = tape_batch_builder,
        command_processor = command_processor,
        metrics_monitor= metrics_monitor,
        disk_space_monitor= disk_space_monitor,
        runtime_stats_calculator= runtime_statistics_calculator,
        db_read_batch_size= config.read_batch_size,
        num_consumers = config.num_consumers,
        consumers_queue_size = config.consumers_queue_size,
        timeout_seconds = config.timeout_seconds
    )

    try:
        processor.run()

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


if __name__ == "__main__":
    main()