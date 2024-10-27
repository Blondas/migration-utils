import argparse
import asyncio

from core import CommandGenerator, CommandExecutor, PerformanceTester
from database.db2_database import DB2Database
from filesystem import local_filesystem as fs
from src.arsadmin_retriever.models.config.config import Config
from logging.file_logger import setup_logger


def generate_commands(config: Config) -> None:
    logger = setup_logger("command_generation", config.log_file)
    database = DB2Database()
    generator = CommandGenerator(config, database, fs, logger)
    commands = generator.generate_commands()
    # Save commands to a file or database for later execution


async def execute_commands(config: Config) -> None:
    logger = setup_logger("command_execution", config.log_file)
    executor = CommandExecutor(config, fs, logger)
    # Load commands from file or database
    commands = []  # Replace with actual loading logic
    await executor.execute_commands(commands)


async def run_performance_test(config: Config) -> None:
    logger = setup_logger("performance_test", config.log_file)
    database = DB2Database()
    generator = CommandGenerator(config, database, fs, logger)
    commands = generator.generate_commands()

    executor = CommandExecutor(config, fs, logger)
    tester = PerformanceTester(config, fs, logger)
    await tester.run_performance_test(executor, commands)


def main() -> None:
    parser = argparse.ArgumentParser(description="`arsadmin retrieve` commands generator and executor")
    parser.add_argument(
        "mode",
        choices=["generate", "execute", "performance-test"],
        help="Operation mode: generate commands, execute commands, or run performance test"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to configuration file"
    )
    args = parser.parse_args()
    config = Config.load_from_yaml(args.config)

    if args.mode == "generate":
        generate_commands(config)
    elif args.mode == "execute":
        asyncio.run(execute_commands(config))
    elif args.mode == "performance-test":
        asyncio.run(run_performance_test(config))


if __name__ == "__main__":
    main()