import logging
from logging.handlers import RotatingFileHandler

def setup_logger():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    log_file_path = 'file.log'
    if not logger.handlers:
        handler = RotatingFileHandler(log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

    return logger

