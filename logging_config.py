import logging
import os

def setup_logging(log_file, error_log_file, include_thread_name=False):
    log_dir = './out/logs'
    os.makedirs(log_dir, exist_ok=True)

    logging_format = '%(asctime)s - %(levelname)s - '
    if include_thread_name:
        logging_format += '%(threadName)s - '
    logging_format += '%(message)s'

    # Configure logging to console and file
    logging.basicConfig(
        level=logging.INFO,
        format=logging_format,
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'{log_dir}/{log_file}')
        ]
    )

    # Set up error logging
    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.ERROR)
    error_handler = logging.FileHandler(f'{log_dir}/{error_log_file}')
    error_handler.setFormatter(logging.Formatter(logging_format))
    error_logger.addHandler(error_handler)

    return logging.getLogger(__name__), error_logger