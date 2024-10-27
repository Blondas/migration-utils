import logging
import logging.config
import yaml
import os


class FileLogger:
    _config_loaded: bool = False

    @staticmethod
    def get_logger(name: str, default_path: str = 'config/logger_config.yaml', default_level: int = logging.INFO,
                   env_key: str = 'LOG_CFG') -> logging.Logger:
        """
        Configure logging (if not already configured) and return a logger with the specified name.

        Args:
            name (str): Name of the logger to return
            default_path (str): Path to the logging configuration file
            default_level (int): Default logging level
            env_key (str): Environment variable that can be used to set the logging config file path

        Returns:
            logging.Logger: Configured logger instance
        """
        if not FileLogger._config_loaded:
            FileLogger._setup_logging(default_path, default_level, env_key)

        return logging.getLogger(name)

    @staticmethod
    def _setup_logging(default_path: str, default_level: int, env_key: str) -> None:
        """
        Setup logging configuration.

        Args:
            default_path (str): Path to the logging configuration file
            default_level (int): Default logging level
            env_key (str): Environment variable that can be used to set the logging config file path
        """
        path: str = os.getenv(env_key, default_path)
        if os.path.exists(path):
            with open(path, 'rt') as f:
                try:
                    config = yaml.safe_load(f.read())
                    FileLogger._create_log_directories(config)
                    logging.config.dictConfig(config)
                    FileLogger._config_loaded = True
                except Exception as e:
                    print(f"Error in Logging Configuration: {e}")
                    print("Using default logging config")
                    logging.basicConfig(level=default_level)
        else:
            logging.basicConfig(level=default_level)
            print("Failed to load configuration file. Using default configs")

        FileLogger._config_loaded = True

    @staticmethod
    def _create_log_directories(config: dict) -> None:
        """
        Create directories for log files if they don't exist.

        Args:
            config (dict): Logging configuration dictionary
        """
        for handler in config.get('handlers', {}).values():
            if 'filename' in handler:
                log_dir: str = os.path.dirname(handler['filename'])
                os.makedirs(log_dir, exist_ok=True)