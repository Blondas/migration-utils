import os
import shutil
import time
import asyncio
import psutil
from statistics import mean
from arsadmin_executor import execute_arsadmin_commands, Config, setup_logger

class PerformanceMetrics:
    def __init__(self):
        self.cpu_wait = []
        self.paging_space = []
        self.page_in = []
        self.page_out = []

    def add_metrics(self, metrics):
        self.cpu_wait.append(metrics['cpu_wait'])
        self.paging_space.append(metrics['paging_space'])
        self.page_in.append(metrics['page_in'])
        self.page_out.append(metrics['page_out'])

    def get_averages(self):
        return {
            'cpu_wait': mean(self.cpu_wait),
            'paging_space': mean(self.paging_space),
            'page_in': mean(self.page_in),
            'page_out': mean(self.page_out)
        }

async def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

async def remove_directory_with_retry(path, executor_logger, max_retries=10, delay=3):
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

def calculate_cpu_iowait_percent():
    # First measurement
    cpu_times_1 = psutil.cpu_times()
    time.sleep(1)  # Wait for 1 second
    # Second measurement
    cpu_times_2 = psutil.cpu_times()

    # Calculate differences
    iowait_diff = cpu_times_2.iowait - cpu_times_1.iowait
    total_diff = sum(cpu_times_2) - sum(cpu_times_1)

    # Calculate percentage
    iowait_percent = (iowait_diff / total_diff) * 100 if total_diff > 0 else 0

    return iowait_percent

async def get_performance_metrics():
    cpu_iowait = calculate_cpu_iowait_percent()
    swap = psutil.swap_memory()

    return {
        'cpu_wait': cpu_iowait,
        'paging_space': swap.used,
        'page_in': psutil.disk_io_counters().read_count,
        'page_out': psutil.disk_io_counters().write_count
    }

async def collect_performance_metrics(interval=5):
    metrics = PerformanceMetrics()
    try:
        while True:
            current_metrics = await get_performance_metrics()
            metrics.add_metrics(current_metrics)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        # Task was cancelled, return collected metrics
        return metrics

async def run_performance_test(config, target_size_gb, executor_logger):
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024

    try:
        await remove_directory_with_retry('./out/data', executor_logger)
        if os.path.exists(config.state_file):
            executor_logger.info(f"Removing state file: {config.state_file}")
            os.remove(config.state_file)
    except Exception as e:
        executor_logger.error(f"Failed to clean up directories: {e}")
        return None, None

    start_time = time.time()

    try:
        executor_task = asyncio.create_task(execute_arsadmin_commands(config, executor_logger))
        metrics_task = asyncio.create_task(collect_performance_metrics())

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
        metrics_task.cancel()

        try:
            await executor_task
        except asyncio.CancelledError:
            pass

        metrics = await metrics_task  # This will now return the collected metrics

        return runtime, metrics
    except Exception as e:
        executor_logger.error(f"Error during performance test: {e}")
        return None, None

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
        runtime, metrics = await run_performance_test(config, target_size_gb, executor_logger)
        if runtime is not None and metrics is not None:
            averages = metrics.get_averages()
            log_entry = (
                f"workers number: {workers}, data size: {target_size_gb}GB, runtime: {runtime:.2f}, \n"
                f" - cpu wait average: {averages['cpu_wait']:.2f}%, \n"
                f" - page in average: {averages['page_in']:.2f}, \n"
                f" - page out average: {averages['page_out']:.2f}, \n"
                f" - paging space 5s_current: {metrics.paging_space}, \n"
                f" - cpu wait 5s_current: {metrics.cpu_wait}, \n"
                f" - page in 5s_current: {metrics.page_in}, \n"
                f" - page out 5s_current: {metrics.page_out}"
            )
            performance_logger.info(log_entry)
        else:
            performance_logger.error(f"Performance test failed for {workers} workers")

    performance_logger.info("Performance testing completed.")

if __name__ == "__main__":
    asyncio.run(main())