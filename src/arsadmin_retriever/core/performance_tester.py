from typing import List

from src.arsadmin_retriever.core.command_executor import CommandExecutor
from src.arsadmin_retriever.models.config.config import Config
from src.arsadmin_retriever.models.command import Command
from src.arsadmin_retriever.interfaces.filesystem_interface import FilesystemInterface
from logging import Logger


class PerformanceTester:
    config: Config
    filesystem: FilesystemInterface
    logger: Logger

    def __init__(self, config: Config, filesystem: FilesystemInterface, logger: Logger):
        self.config = config
        self.filesystem = filesystem
        self.logger = logger

    async def run_performance_test(self, executor: CommandExecutor, commands: List[Command]) -> None:
        # Implement the performance testing logic here
        # This should be similar to the run_performance_test function in the original code
        pass