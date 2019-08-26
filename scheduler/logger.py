import logging
import sys

def get_logger():
    formatter = logging.Formatter('%(asctime)-15s %(levelname)-9s - %(message)s')

    logger = logging.getLogger('scheduler')
    logger.setLevel(logging.DEBUG)

    info_handler = logging.StreamHandler(sys.stdout)
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(formatter)

    warn_handler = logging.StreamHandler(sys.stderr)
    warn_handler.setLevel(logging.WARN)
    warn_handler.setFormatter(formatter)

    logger.addHandler(info_handler)
    logger.addHandler(warn_handler)
    
    return logger
