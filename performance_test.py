import os
import shutil
import time
import subprocess
import psutil
from logging_config import setup_logging

def get_directory_size(path):
    """Calculate the total size of a directory in bytes."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def run_performance_test(threads, target_size_gb=5, command_file='./out/arsadmin_commands.txt'):
    """Run a performance test for a given number of threads."""
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024  # Convert GB to bytes
    
    # Delete ./out/data directory and state file
    if os.path.exists('./out/data'):
        shutil.rmtree('./out/data')
    if os.path.exists('./out/execution_state.json'):
        os.remove('./out/execution_state.json')
    
    # Start the command executor script
    process = subprocess.Popen(['python', 'command_executor.py',
                                '--threads', str(threads),
                                '--command_file', command_file])
    
    start_time = time.time()
    
    # Monitor the size of ./out/data directory
    while True:
        if not os.path.exists('./out/data'):
            time.sleep(1)
            continue
        
        current_size = get_directory_size('./out/data')
        if current_size >= target_size_bytes:
            break
        
        time.sleep(5)  # Check every 5 seconds
    
    end_time = time.time()
    runtime = end_time - start_time
    
    # Stop the script
    parent = psutil.Process(process.pid)
    for child in parent.children(recursive=True):
        child.terminate()
    parent.terminate()
    
    return runtime

def main():
    logger, _ = setup_logging('performance_test.log', 'performance_test_error.log')
    thread_counts = [1, 2, 3, 4, 5]
    target_size_gb = 5
    
    for threads in thread_counts:
        runtime = run_performance_test(threads, target_size_gb)
        logger.info(f"{threads},{target_size_gb},{runtime:.2f}")
        
    logger.info("Performance testing completed.")

if __name__ == "__main__":
    main()
