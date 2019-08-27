import logging
import sys
from scheduler.config import config

cfg = config()

def get_logger():

    log_level = logging.DEBUG if cfg.get('log_level') == 'debug' else logging.INFO 

    formatter = logging.Formatter('%(asctime)-15s %(levelname)-9s - %(message)s')

    logger = logging.getLogger('scheduler')
    logger.setLevel(log_level)

    info_handler = logging.StreamHandler(sys.stdout)
    info_handler.setLevel(log_level)
    info_handler.setFormatter(formatter)

    warn_handler = logging.StreamHandler(sys.stderr)
    warn_handler.setLevel(logging.WARN)
    warn_handler.setFormatter(formatter)

    logger.addHandler(info_handler)
    logger.addHandler(warn_handler)
    
    return logger
