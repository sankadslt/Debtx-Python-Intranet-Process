from pathlib import Path

def get_project_root():
    """
    Returns the project root directory dynamically.
    Assumes this script is inside the project directory.
    """
    return Path(__file__).resolve().parent.parent.parent 

def get_config_filePath():    
    """
    Returns the full path to the INI configuration file located in the Config directory.
    """
    project_root = get_project_root()
    config_file_path = project_root / "Config" / "drs_core_config.ini"
    if not config_file_path.exists():
        raise FileNotFoundError(f"Config file not found at: {config_file_path}")
    return config_file_path

def get_logger_filePath():    
    """
    Returns the full path to the logging configuration file located in the Config directory.
    """
    project_root = get_project_root()
    print (f"Project root: {project_root}")
    logger_file_path = project_root / "Config" / "logConfig.ini"
    if not logger_file_path.exists():
        raise FileNotFoundError(f"Logger config file not found at: {logger_file_path}")
    return logger_file_path