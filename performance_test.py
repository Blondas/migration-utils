import os
import shutil
import time
import asyncio
import psutil
from logging_config import setup_logging
from arsadmin_executor import execute_arsadmin_commands, Config


async def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


async def run_performance_test(max_workers, target_size_gb=5, command_file='./out/arsadmin_commands.txt'):
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024

    if os.path.exists('./out/data'):
        shutil.rmtree('./out/data')
    if os.path.exists('./out/execution_state.json'):
        os.remove('./out/execution_state.json')

    config = Config(
        command_file=command_file,
        state_file='./out/execution_state.json',
        log_file='performance_test.log',
        err_log_file='performance_test_error.log',
        min_free_space_percent=10.0,
        max_workers=max_workers,
        save_interval=60
    )

    start_time = time.time()

    executor_task = asyncio.create_task(execute_arsadmin_commands(config))

    while True:
        if not os.path.exists('./out/data'):
            await asyncio.sleep(1)
            continue

        current_size = await get_directory_size('./out/data')
        if current_size >= target_size_bytes or executor_task.done():
            break

        await asyncio.sleep(5)

    end_time = time.time()
    runtime = end_time - start_time

    executor_task.cancel()
    try:
        await executor_task
    except asyncio.CancelledError:
        pass

    return runtime


async def main():
    logger, _ = setup_logging('performance_test.log', 'performance_test_error.log')
    worker_counts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    target_size_gb = 5

    for workers in worker_counts:
        runtime = await run_performance_test(workers, target_size_gb)
        logger.info(f"{workers},{target_size_gb},{runtime:.2f}")

    logger.info("Performance testing completed.")


if __name__ == "__main__":
    asyncio.run(main())