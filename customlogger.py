#!/usr/bin/env python3

import os
import logging
import datetime


def logger(name):
    """
    Creates file handler logger to log messages.
    Log messages are outputted to a logs directory (creates the log directory if it does not exist) in the format:
    YYYY-MM-DD-HH-MM.log
    
    Args:
        name (str): Logger name.

    Returns:
        class 'logging.Logger'
    """

    today = datetime.datetime.today().strftime("%Y-%m-%d_%H-%M")
    if not os.path.exists("logs"):
        os.mkdir("logs")
    logpath = os.path.join("logs", today + ".log")
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    filehandler = logging.FileHandler(logpath)
    filehandler.setLevel(logging.DEBUG)

    logger_format = logging.Formatter("%(asctime)s | %(name)s | %(funcName)s | %(levelname)s | %(lineno)d | %(message)s")
    filehandler.setFormatter(logger_format)
    logger.addHandler(filehandler)

    return logger