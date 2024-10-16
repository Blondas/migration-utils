import os
import shutil
import time
import asyncio
from arsadmin_executor import execute_arsadmin_commands, Config, setup_logger


async def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


async def remove_directory_with_retry(path, executor_logger, max_retries=5, delay=1):
    executor_logger.info(f"Attempting to remove directory: {path}")
    executor_logger.info(f"Directory exists before removal attempt: {os.path.exists(path)}")

    if not os.path.exists(path):
        executor_logger.info(f"Directory {path} does not exist. No need to remove.")
        return

    for attempt in range(max_retries):
        try:
            shutil.rmtree(path)
            executor_logger.info(f"Successfully removed directory {path}")
            return
        except Exception as e:
            executor_logger.warning(f"Attempt {attempt + 1} failed to remove directory {path}. Error: {str(e)}")
            if attempt == max_retries - 1:
                executor_logger.error(f"Failed to remove directory {path} after {max_retries} attempts.")
                raise
            await asyncio.sleep(delay)


async def run_performance_test(config, target_size_gb, executor_logger):
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024

    try:
        await remove_directory_with_retry('./out/data', executor_logger)
        if os.path.exists(config.state_file):
            executor_logger.info(f"Removing state file: {config.state_file}")
            os.remove(config.state_file)
    except Exception as e:
        executor_logger.error(f"Failed to clean up directories: {e}")
        return None

    start_time = time.time()

    try:
        executor_task = asyncio.create_task(execute_arsadmin_commands(config, executor_logger))

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
    except Exception as e:
        executor_logger.error(f"Error during performance test: {e}")
        return None


async def main():
    performance_logger = setup_logger('performance_test', './out/log/performance_test.log')
    executor_logger = setup_logger('command_executor', './out/log/command_executor.log')

    worker_counts = [1, 2]
    target_size_gb = 5

    for workers in worker_counts:
        config = Config(
            command_file='./out/arsadmin_commands.txt',
            state_file='./out/execution_state.json',
            min_free_space_percent=10.0,
            max_workers=workers,
            save_interval=60
        )
        runtime = await run_performance_test(config, target_size_gb, executor_logger)
        if runtime is not None:
            performance_logger.info(f"workers: {workers}, test data size: {target_size_gb} GB, runtime: {runtime:.2f}")
        else:
            performance_logger.error(f"Performance test failed for {workers} workers")

    performance_logger.info("Performance testing completed.")


if __name__ == "__main__":
    asyncio.run(main())