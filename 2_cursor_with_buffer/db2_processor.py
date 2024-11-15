from pathlib import Path

import ibm_db_dbi
import threading
from queue import Queue, Empty
from typing import List, Dict, Optional, Tuple, Iterator, Set, NamedTuple
from contextlib import contextmanager
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import os
import logging
import re
import subprocess
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    # Database
    database: str
    hostname: str
    port: int
    protocol: str
    db_user: str
    db_password: str
    schema: str

    # Producer Consumer setup
    read_batch_size: int
    num_consumers: int
    consumers_queue_size: int

    # DB updater setup
    update_batch_size: int
    update_queue_size: int
    update_interval_seconds: int

    # Arsadmin setup
    command_max_objects: int
    dir_max_elems: int
    user: str
    password: Optional[str]
    od_inst: str
    base_dir: str
    table_name: str

    @property
    def connection_string(self) -> str:
        return (
            f"DATABASE={self.database};"
            f"HOSTNAME={self.hostname};"
            f"PORT={self.port};"
            f"PROTOCOL={self.protocol};"
            f"UID={self.db_user};"
            f"PWD={self.db_password};"
        )


@dataclass
class ProcessingMetrics:
    batches_produced: int = 0
    batches_processed: int = 0
    rows_processed: int = 0
    start_time: Optional[datetime] = None

    def log_metrics(self) -> None:
        if not self.start_time:
            return

        duration: float = (datetime.now() - self.start_time).total_seconds()
        logger.info(
            f"Processed {self.rows_processed:,} rows in {duration:.2f}s "
            f"({self.rows_processed / duration:.0f} rows/s). "
            f"Batches: produced={self.batches_produced:,}, "
            f"processed={self.batches_processed:,}"
        )


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


class TapeCommandsBuilder:
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
            str((self._current_batch_no // self.dir_max_elems) + 1),
            str(command_subdir)
        ))

    def build_tape_commands(
        self,
        rows: List[DBRow]
    ) -> List[Command]:
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


class DB2Connection:
    def __init__(self, conn_string: str, for_updates: bool = False) -> None:
        self.conn_string: str = conn_string
        self.for_updates: bool = for_updates

    @contextmanager
    def get_cursor(self) -> Iterator[ibm_db_dbi.Cursor]:
        conn: ibm_db_dbi.Connection = ibm_db_dbi.connect(self.conn_string)
        try:
            cursor: ibm_db_dbi.Cursor = conn.cursor()

            if self.for_updates:
                cursor.execute("SET CURRENT ISOLATION = CS")
                cursor.execute("SET CURRENT LOCK TIMEOUT = 30")
            else:
                cursor.execute("SET CURRENT ISOLATION = UR")
                cursor.execute("SET CURRENT QUERY OPTIMIZATION = 5")
                cursor.execute("SET CURRENT DEGREE = 'ANY'")

            yield cursor

            if self.for_updates:
                conn.commit()
        except Exception as e:
            if self.for_updates:
                conn.rollback()
            raise e
        finally:
            conn.close()


class StatusUpdateManager:
    def __init__(
            self,
            db: DB2Connection,
            table_name: str,
            batch_size: int,
            queue_size: int,
            update_interval_seconds: int
    ) -> None:
        self.db = db
        self.table_name = table_name
        self.batch_size: int = batch_size
        self.queue: Queue[Optional[StatusUpdate]] = Queue(queue_size)
        self.update_interval_seconds: int = update_interval_seconds
        self.shutdown_event: threading.Event = threading.Event()
        self.update_thread: Optional[threading.Thread] = None

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
        self.queue.put(status_update)

    def _update_status_worker(self) -> None:
        pending_updates: List[StatusUpdate] = []
        last_update_time: float = time.time()

        while not self.shutdown_event.is_set():
            try:
                queue_query_interval_seconds: float = 1.0
                update: Optional[StatusUpdate] = self.queue.get(timeout=queue_query_interval_seconds)
                if update is None:
                    break

                pending_updates.append(update)

                if (len(pending_updates) >= self.batch_size or
                        time.time() - last_update_time > self.update_interval_seconds):
                    self._process_updates(pending_updates)
                    pending_updates = []
                    last_update_time = time.time()

            except Empty:
                if pending_updates:
                    self._process_updates(pending_updates)
                    pending_updates = []
                    last_update_time = time.time()

            except Exception as e:
                logger.error(f"Status update worker failed: {e}")
                for pending_update in pending_updates:
                    self.queue.put(pending_update)
                time.sleep(1)

        if pending_updates:
            try:
                self._process_updates(pending_updates)
            except Exception as e:
                logger.error(f"Final status update failed: {e}")

    def _process_updates(self, updates: List[StatusUpdate]) -> None:
        updates_by_status: Dict[ProcessingStatus, Set[int]] = {}
        for update in updates:
            if update.status not in updates_by_status:
                updates_by_status[update.status] = set()
            updates_by_status[update.status].update(update.ids)

        try:
            with self.db.get_cursor() as cursor:
                for status, ids in updates_by_status.items():
                    for i in range(0, len(ids), self.batch_size):
                        batch = list(ids)[i:i + self.batch_size]
                        id_values: str = ",".join(str(idx) for idx in batch)

                        base_sql = f"""
                        UPDATE {self.table_name}
                        SET STATUS = ?, 
                        DTSTAMP = CURRENT TIMESTAMP
                        WHERE ID IN ({id_values})
                        """
                        cursor.execute(base_sql, (status.value,))

            # logger.info(
            #     f"Updated status for {sum(len(b.ids) for b in updates)} "
            #     f"records across {len(updates)} batches"
            # )
        except Exception as e:
            logger.error(
                f"Status update failed, error: {e}"
            )
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


class DB2DataProcessor:
    def __init__(
            self,
            read_db: DB2Connection,
            status_update_manager: StatusUpdateManager,
            metrics: ProcessingMetrics,
            command_batch_builder: TapeCommandsBuilder,
            command_processor: CommandProcessor,
            db_read_batch_size: int,
            num_consumers: int,
            consumers_queue_size: int,
    ) -> None:
        self.read_db = read_db
        self.status_update_manager = status_update_manager
        self.metrics = metrics
        self.command_batch_builder = command_batch_builder
        self.command_processor = command_processor

        self.db_read_batch_size = db_read_batch_size
        self.num_consumers = num_consumers
        self.queue: Queue[Optional[List[Command]]] = Queue(maxsize=consumers_queue_size)
        self.shutdown_event: threading.Event = threading.Event()

    def _produce(
            self,
            rows: List[DBRow]
    ) -> None:
        """Process a complete group of rows with the same tape_id"""
        if not rows:
            return

        # Create tape commands for the group
        tape_commands: List[Command] = self.command_batch_builder.build_tape_commands(rows)

        # Update status for all objects
        status_update = StatusUpdate(
            ids= {
                obj.db_record_id
                for cmd in tape_commands
                for obj in cmd.object_records
            },
            status=ProcessingStatus.STARTED
        )
        self.status_update_manager.queue_update(status_update)

        # Queue the tape commands
        self.queue.put(tape_commands)
        self.metrics.batches_produced += 1

        logger.info(
            f"Processed tape group for tape_id {rows[0].tape_id}: "
            f"{len(rows)} records, {len(tape_commands)} commands"
        )

    def producer(self, table_name: str) -> None:
        try:
            with self.read_db.get_cursor() as cursor:
                query: str = f"""
                    SELECT 
                        ID,
                        ODSLOC,
                        ODCREATS,
                        AGID_NAME,
                        AGNAME,
                        LOADID,
                        PRINID,
                        STATUS,
                        DTSTAMP
                    FROM 
                        {table_name}
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
                # query: str = f"""
                #     SELECT
                #         od_inst,
                #         object_name,
                #         pri_nid,
                #         tape_id,
                #         status,
                #         dest_subdir,
                #         retrieve_dt
                #     FROM
                #         {table_name}
                #     WHERE
                #         STATUS = '{ProcessingStatus.PENDING.value}'
                #     ORDER BY
                #         tape_id,
                #         pri_nid
                #     --#SET ISOLATION = UR
                #     OPTIMIZE FOR {self.db_read_batch_size} ROWS
                # """

                cursor.execute(query)

                buffer: List[DBRow] = []
                current_tape_id: Optional[str] = None

                while True:
                    if self.shutdown_event.is_set():
                        break

                    rows = cursor.fetchmany(self.db_read_batch_size)
                    if not rows:
                        # Process any remaining buffered rows
                        if buffer:
                            self._produce(buffer)
                        break

                    db_rows: list[DBRow] = [DBRow(*row) for row in rows]

                    for row in db_rows:
                        if current_tape_id is None:
                            current_tape_id = row.tape_id

                        if row.tape_id != current_tape_id:
                            # Process complete tape group
                            self._produce(buffer)
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

    def consumer(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                tape_commands: Optional[List[Command]] = self.queue.get()
                if tape_commands is None:
                    break

                for command in tape_commands:
                    try:
                        command_result: CommandResult = self.command_processor.process_command(command)

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


def load_config(config_path: Optional[str] = None) -> Config:
    """Load config from yaml file"""
    if config_path is None:
        config_path = str(Path(__file__).parent / 'config.yaml')

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    return Config(
        # Database
        database=yaml_config['database']['database'],
        hostname=yaml_config['database']['hostname'],
        port=yaml_config['database']['port'],
        protocol=yaml_config['database']['protocol'],
        db_user=yaml_config['database']['db_user'],
        db_password=yaml_config['database']['db_password'],
        schema=yaml_config['database']['schema'],

        # Consumer
        read_batch_size=yaml_config['consumer']['read_batch_size'],
        num_consumers=yaml_config['consumer']['num_consumers'],
        consumers_queue_size=yaml_config['consumer']['consumers_queue_size'],

        # Updater
        update_batch_size=yaml_config['updater']['update_batch_size'],
        update_queue_size=yaml_config['updater']['update_queue_size'],
        update_interval_seconds=yaml_config['updater']['update_interval_seconds'],

        # Arsadmin
        command_max_objects=yaml_config['arsadmin']['command_max_objects'],
        dir_max_elems=yaml_config['arsadmin']['dir_max_elems'],
        user=yaml_config['arsadmin']['user'],
        password=yaml_config['arsadmin'].get('password'),  # Optional
        od_inst=yaml_config['arsadmin']['od_inst'],
        base_dir=yaml_config['arsadmin']['base_dir'],
        table_name=yaml_config['arsadmin']['table_name']
    )


def main() -> None:
    config: Config = load_config()

    conn_string: str = f'DATABASE={config.database};HOSTNAME={config.hostname};PORT={config.port};PROTOCOL={config.protocol};UID={config.db_user};PWD={config.db_password};'

    read_db: DB2Connection = DB2Connection(conn_string, for_updates=False)
    update_db: DB2Connection = DB2Connection(conn_string, for_updates=True)
    metrics: ProcessingMetrics = ProcessingMetrics()
    tape_batch_builder: TapeCommandsBuilder = TapeCommandsBuilder(
        command_max_objects = config.command_max_objects,
        dir_max_elems = config.dir_max_elems,
        user = config.user,
        password = config.password,
        od_inst = config.od_inst,
        base_dir= config.base_dir,
    )
    status_update_manager: StatusUpdateManager = StatusUpdateManager(
        db = update_db,
        table_name= config.table_name,
        batch_size = config.update_batch_size,
        queue_size= config.update_queue_size,
        update_interval_seconds = config.update_interval_seconds
    )
    command_processor = CommandProcessor()
    processor = DB2DataProcessor(
        read_db = read_db,
        status_update_manager = status_update_manager,
        metrics = metrics,
        command_batch_builder = tape_batch_builder,
        command_processor = command_processor,
        db_read_batch_size= config.read_batch_size,
        num_consumers = config.num_consumers,
        consumers_queue_size = config.consumers_queue_size
    )

    try:
        processor.run()
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


if __name__ == "__main__":
    main()