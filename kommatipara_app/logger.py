# Importing necessary libraries
import logging
from logging.handlers import RotatingFileHandler

def setup_logger():
    """
    Configures and sets up a logger with a rotating file handler.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Configure the root logger to display log messages at the INFO level
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Specify the path for the log file
    log_file_path = 'file.log'

    # Check if the logger already has handlers to avoid duplicate handlers
    if not logger.handlers:
        # Create a rotating file handler with a maximum log file size of 10 MB
        handler = RotatingFileHandler(log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5)
        
        # Set the log message format for the handler
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # Add the handler to the logger
        logger.addHandler(handler)

    # Return the configured logger
    return logger


