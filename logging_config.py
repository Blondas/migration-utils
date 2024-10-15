import logging
from typing import Tuple

def setup_logging(log_file: str, err_log_file: str) -> Tuple[logging.Logger, logging.Logger]:
    logger = logging.getLogger()
    error_logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    error_logger.setLevel(logging.ERROR)

    file_handler = logging.FileHandler(log_file)
    error_file_handler = logging.FileHandler(err_log_file)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    error_file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    error_logger.addHandler(error_file_handler)

    return logger, error_logger