import asyncio
import os
import shutil
import re
from dataclasses import dataclass
from typing import List, Set, Optional, Tuple
import aiofiles
import json
from collections import deque
import argparse
import logging

@dataclass
class Config:
    command_file: str
    state_file: str
    min_free_space_percent: float
    max_workers: int
    save_interval: int


@dataclass
class Command:
    index: int
    base_command: str
    doc_names: List[str]
    output_dir: str


class CommandState:
    def __init__(self):
        self.pending = deque()
        self.in_progress: Set[int] = set()
        self.completed: Set[int] = set()
        self.state_lock = asyncio.Lock()

    async def update_state(self, command_index: int, new_status: str) -> None:
        async with self.state_lock:
            if new_status == 'in_progress':
                self.pending.remove(command_index)
                self.in_progress.add(command_index)
            elif new_status == 'completed':
                self.in_progress.remove(command_index)
                self.completed.add(command_index)

    async def get_next_command(self) -> Optional[int]:
        async with self.state_lock:
            if not self.pending:
                return None
            command_index = self.pending.popleft()
            self.in_progress.add(command_index)
            return command_index

    async def initialize_pending(self, all_commands: List[int]) -> None:
        async with self.state_lock:
            # First, re-queue all in_progress commands
            self.pending.extend(self.in_progress)
            # Then add any commands not in completed or already added from in_progress
            self.pending.extend(cmd for cmd in all_commands
                                if cmd not in self.completed and cmd not in self.pending)
            # Clear the in_progress set as we've re-queued these commands
            self.in_progress.clear()


class StateManager:
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.state = CommandState()

    async def save_state(self) -> None:
        async with self.state.state_lock:
            state_data = {
                "pending": list(self.state.pending),
                "in_progress": list(self.state.in_progress),
                "completed": list(self.state.completed)
            }
            async with aiofiles.open(self.state_file, 'w') as f:
                await f.write(json.dumps(state_data, indent=2))

    async def load_state(self) -> None:
        if os.path.exists(self.state_file):
            async with aiofiles.open(self.state_file, 'r') as f:
                data = json.loads(await f.read())
                async with self.state.state_lock:
                    self.state.pending = deque(data.get("pending", []))
                    self.state.in_progress = set(data.get("in_progress", []))
                    self.state.completed = set(data.get("completed", []))


def setup_logger(name, log_file, level=logging.INFO):
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Usage
logger = setup_logger('my_logger', 'app.log')
logger.info('This is a log message')


async def get_free_space_percent() -> float:
    total, used, free = shutil.disk_usage("/odlahd")
    return (free / total) * 100


async def ensure_directory_exists(directory: str, logger: logging.Logger) -> None:
    os.makedirs(directory, exist_ok=True)
    # logger.info(f"Ensured directory exists: {directory}")


async def periodic_save(state_manager: StateManager, interval: int) -> None:
    while True:
        await asyncio.sleep(interval)
        await state_manager.save_state()


async def parse_command(command: str) -> Command:
    parts = command.split()
    base_idx = parts.index("-d")
    base_command = " ".join(parts[:base_idx + 2])
    doc_names = parts[base_idx + 2:]
    output_dir = parts[base_idx + 1]
    return Command(-1, base_command, doc_names, output_dir)


async def execute_command(command: str, logger: logging.Logger) -> Tuple[int, str, str]:
    # logger.info(f"Executing command: `{command}`")
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode(), stderr.decode()


async def process_command(command: Command, logger: logging.Logger) -> bool:
    await ensure_directory_exists(command.output_dir, logger)

    while command.doc_names:
        full_command = f"{command.base_command} {' '.join(command.doc_names)}"
        return_code, stdout, stderr = await execute_command(full_command, logger)

        if return_code != 0:
            if "ARS1159E Unable to retrieve the object" in stderr:
                match = re.search(r"Unable to retrieve the object >(\S+)<", stderr)
                if match:
                    failing_doc = match.group(1)
                    logger.error(
                        f"code: {return_code}, document: {failing_doc}, message: Unable to retrieve document"
                        f", skipping current document and re-executing , command: `{full_command}`")
                    command.doc_names = command.doc_names[command.doc_names.index(failing_doc) + 1:]
                else:
                    logger.error(f"code: {return_code}, message: Could not identify failing document"
                                       f", skipping remaining documents in this command, command: `{full_command}`")
                    return False
            elif "ARS1168E Unable to determine Storage Node" in stderr:
                match = re.search(r"-n (\d+-\d+)", full_command)
                storage_node = ", storage node: " + match.group(1) if match else ""
                logger.error(f"code: {return_code}{storage_node}, message: Unable to determine Storage Node"
                                   f", skipping remaining documents in this command, command: `{full_command}`")
                return False
            elif "ARS1110E The application group" in stderr:
                logger.error(f"code: {return_code}, message: The Application Group (or permission) doesn't exist"
                                   f", skipping remaining documents in this command, command: `{full_command}`")
                return False
            else:
                logger.error(f"code: {return_code}, message: {stderr}"
                                   f", skipping remaining documents in this command, command: `{full_command}`")
                return False
        else:
            # logger.info(f"Command executed successfully, command: `{full_command}`")
            return True

    return True


async def worker(state_manager: StateManager, commands: List[Command], logger: logging.Logger, min_free_space_percent: float) -> None:
    while True:
        if await get_free_space_percent() < min_free_space_percent:
            logger.warning(f"Free disk space below {min_free_space_percent}%. Stopping execution.")
            break

        command_index = await state_manager.state.get_next_command()
        if command_index is None:
            break

        command = commands[command_index]

        try:
            await process_command(command, logger)
            await state_manager.state.update_state(command_index, 'completed')
        except Exception as e:
            logger.error(f"Unexpected error occurred in command {command_index}: {str(e)}", exc_info=True)


async def execute_arsadmin_commands(config: Config, logger: logging.Logger) -> None:
    state_manager = StateManager(config.state_file)
    await state_manager.load_state()

    async with aiofiles.open(config.command_file, 'r') as f:
        commands = [await parse_command(line.strip()) for line in await f.readlines() if line.strip()]

    for i, command in enumerate(commands):
        command.index = i

    await state_manager.state.initialize_pending(range(len(commands)))

    workers = [asyncio.create_task(worker(state_manager, commands, logger, config.min_free_space_percent))
               for _ in range(config.max_workers)]

    save_task = asyncio.create_task(periodic_save(state_manager, config.save_interval))

    await asyncio.gather(*workers)
    save_task.cancel()

    await state_manager.save_state()  # Final save
    logger.info("Finished executing commands")


async def main() -> None:
    parser = argparse.ArgumentParser(description='ArsAdmin Command Executor')
    parser.add_argument('--command_file', default='./out/arsadmin_commands.txt', help='Path to the command file')
    parser.add_argument('--state_file', default='./out/execution_state.json', help='Path to the state file')
    parser.add_argument('--log_file', default='./out/log/command_executor.log', help='Path to the log file')
    parser.add_argument('--min_free_space_percent', type=float, default=10.0, help='Minimum free space percentage')
    parser.add_argument('--max_workers', type=int, default=8, help='Maximum number of worker tasks')
    parser.add_argument('--save_interval', type=int, default=60, help='State save interval in seconds')
    args = parser.parse_args()

    config = Config(
        command_file=args.command_file,
        state_file=args.state_file,
        min_free_space_percent=args.min_free_space_percent,
        max_workers=args.max_workers,
        save_interval=args.save_interval
    )

    logger = setup_logger("arsadmin_executor", args.log_file, logging.INFO)

    await execute_arsadmin_commands(config, logger)


if __name__ == "__main__":
    asyncio.run(main())