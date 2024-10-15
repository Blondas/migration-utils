import logging
from typing import Tuple
import os

def setup_logging(log_file: str, err_log_file: str) -> Tuple[logging.Logger, logging.Logger]:
    log_dir = './out/logs'
    os.makedirs(log_dir, exist_ok=True)

    logging_format = '%(asctime)s - %(levelname)s - %(message)s'

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
    error_handler = logging.FileHandler(f'{log_dir}/{err_log_file}')
    error_handler.setFormatter(logging.Formatter(logging_format))
    error_logger.addHandler(error_handler)

    return logging.getLogger(__name__), error_logger