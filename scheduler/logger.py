import logging
import sys
from scheduler.config import config

cfg = config()

class LogFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, logRecord):
        return logRecord.levelno <= self.__level

def get_logger():

    log_level = logging.DEBUG if cfg.get('log_level') == 'debug' else logging.INFO 

    formatter = logging.Formatter('%(asctime)-15s %(levelname)-9s - %(message)s')

    logger = logging.getLogger('scheduler')
    logger.setLevel(log_level)

    info_handler = logging.StreamHandler(sys.stdout)
    info_handler.setLevel(log_level)
    info_handler.setFormatter(formatter)
    info_handler.addFilter(LogFilter(logging.INFO))

    warn_handler = logging.StreamHandler(sys.stderr)
    warn_handler.setLevel(logging.WARN)
    warn_handler.setFormatter(formatter)

    # prevents duplicate log entries
    if (logger.hasHandlers()):
        logger.handlers.clear()

    logger.addHandler(info_handler)
    logger.addHandler(warn_handler)
    
    return logger
