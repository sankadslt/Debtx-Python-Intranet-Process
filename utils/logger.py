from pathlib import Path
import os
import logging
import logging.config
import configparser

class SingletonLogger:
    _instances = {}
    _configured = False

    @classmethod
    def configure(cls):
        project_root = Path(__file__).resolve().parents[1]
        config_dir = project_root / 'config'
        corefig_path = config_dir / 'core_config.ini'

        if not corefig_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {corefig_path}")

        config = configparser.ConfigParser()
        config.read(str(corefig_path))

        # Get current environment
        if 'environment' not in config or 'current' not in config['environment']:
            raise ValueError("Missing [environment] section or 'current' key in corefig.ini")
        environment = config['environment']['current'].lower()

        # Get logger path based on environment
        logger_section = f'logger_path_{environment}'
        if logger_section not in config or 'log_dir' not in config[logger_section]:
            raise ValueError(f"Missing 'log_dir' under section [{logger_section}]")

        log_dir = Path(config[logger_section]['log_dir'])
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = (log_dir / "default.log").as_posix()  # Use as_posix() for consistent formatting

        print(f"Logger Path: {log_file_path} (env: {environment})")

        # Load logger.ini with dynamic path
        logger_ini_path = config_dir / 'logger.ini'
        if not logger_ini_path.exists():
            raise FileNotFoundError(f"Logger configuration file not found: {logger_ini_path}")

        logging.config.fileConfig(
            str(logger_ini_path),
            defaults={'logfilename': log_file_path},  # Pass normalized path
            disable_existing_loggers=False
        )
        cls._configured = True

    @classmethod
    def get_logger(cls, logger_name='appLogger'):
        if not cls._configured:
            raise ValueError("Logger not configured. Please call 'configure()' first.")

        if logger_name not in cls._instances:
            cls._instances[logger_name] = logging.getLogger(logger_name)

        return cls._instances[logger_name]