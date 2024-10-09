import os
import subprocess
import threading
import queue
import logging
import time
import psutil
from concurrent.futures import ThreadPoolExecutor
import argparse
import json

# Set up logging
def setup_logging():
    log_dir = './out/logs'
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'{log_dir}/executor.log')
        ]
    )
    
    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.ERROR)
    error_handler = logging.FileHandler(f'{log_dir}/executor_error.log')
    error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    error_logger.addHandler(error_handler)
    
    return logging.getLogger(__name__), error_logger

logger, error_logger = setup_logging()

# Configuration
CONFIG = {
    'num_threads': 8,
    'min_disk_space_percent': 10,
    'command_file': './out/arsadmin_retrieve.txt',
    'progress_file': './out/progress.json',
    'performance_log': './out/performance_log.json'
}

def get_free_disk_space_percent():
    """Get the percentage of free disk space."""
    disk = psutil.disk_usage('/')
    return disk.free / disk.total * 100

def check_disk_space():
    """Check if there's enough free disk space."""
    free_space = get_free_disk_space_percent()
    if free_space < CONFIG['min_disk_space_percent']:
        logger.warning(f"Low disk space: {free_space:.2f}% free. Pausing new threads.")
        return False
    return True

def execute_command(command, command_index):
    """Execute a single arsadmin retrieve command."""
    try:
        logger.info(f"Executing command {command_index}: {command}")
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Command {command_index} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        error_logger.error(f"Command {command_index} failed with return code {e.returncode}: {e.stderr}")
        return False

def process_commands(command_queue, progress, lock):
    """Process commands from the queue."""
    while True:
        if not check_disk_space():
            time.sleep(60)  # Wait for a minute before checking again
            continue

        try:
            command_index, command = command_queue.get_nowait()
        except queue.Empty:
            break

        success = execute_command(command, command_index)

        with lock:
            progress['completed'].append(command_index)
            progress['last_completed'] = command_index
            if not success:
                progress['failed'].append(command_index)
            
            # Save progress
            with open(CONFIG['progress_file'], 'w') as f:
                json.dump(progress, f)

        command_queue.task_done()

def load_commands():
    """Load commands from the file."""
    with open(CONFIG['command_file'], 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_progress():
    """Load progress from the progress file."""
    if os.path.exists(CONFIG['progress_file']):
        with open(CONFIG['progress_file'], 'r') as f:
            return json.load(f)
    return {'completed': [], 'failed': [], 'last_completed': -1}

def main(num_threads=CONFIG['num_threads']):
    commands = load_commands()
    progress = load_progress()
    command_queue = queue.Queue()

    # Enqueue commands that haven't been completed yet
    for i, command in enumerate(commands):
        if i > progress['last_completed'] and i not in progress['completed']:
            command_queue.put((i, command))

    lock = threading.Lock()

    start_time = time.time()
    total_size = 0

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for _ in range(num_threads):
            executor.submit(process_commands, command_queue, progress, lock)

    command_queue.join()

    end_time = time.time()
    execution_time = end_time - start_time

    # Calculate total size of downloaded data
    for root, dirs, files in os.walk('./out/data'):
        total_size += sum(os.path.getsize(os.path.join(root, name)) for name in files)

    total_size_gb = total_size / (1024 * 1024 * 1024)  # Convert to GB

    logger.info(f"All commands processed. Total execution time: {execution_time:.2f} seconds")
    logger.info(f"Total data downloaded: {total_size_gb:.2f} GB")
    logger.info(f"Download speed: {total_size_gb / (execution_time / 3600):.2f} GB/hour")

    # Log performance data
    performance_data = {
        'num_threads': num_threads,
        'execution_time': execution_time,
        'total_size_gb': total_size_gb,
        'download_speed_gb_per_hour': total_size_gb / (execution_time / 3600)
    }

    with open(CONFIG['performance_log'], 'a') as f:
        json.dump(performance_data, f)
        f.write('\n')

def run_performance_tests():
    """Run performance tests with different numbers of threads."""
    thread_counts = [2, 4, 8, 12, 16]
    for count in thread_counts:
        logger.info(f"Running performance test with {count} threads")
        main(num_threads=count)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute arsadmin retrieve commands with multi-threading.")
    parser.add_argument('--performance-test', action='store_true', help="Run performance tests")
    parser.add_argument('--threads', type=int, default=CONFIG['num_threads'], help="Number of threads to use")
    args = parser.parse_args()

    if args.performance_test:
        run_performance_tests()
    else:
        main(num_threads=args.threads)
