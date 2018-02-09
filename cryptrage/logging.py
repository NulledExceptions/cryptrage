import logging
import logging.config
from logging import Logger  # type hinting
from typing import Dict
from os import path
from functools import wraps

import yaml


def setup_logging(*, config_path: str, name: str, default_level: int=logging.INFO) -> Logger:
    if config_path and path.exists(config_path):
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basisConfig(level=default_level)
    return logging.getLogger(name)


def add_logger(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        logger = kwargs.get('logger')
        if not logger:
            logger = logging.getLogger(__name__)
        result = function(*args, **{**kwargs, "logger": logger})
        return result
    return wrapper


def strip_log_kwargs(kwargs: Dict) -> Dict:
    """
    Only retain keyword arguments that makes sense to logger.debug

    :param kwargs: A dictionary
    :return: A dictionary with, if available, `valid_arguments` as keys
    """
    valid_arguments = ["exc_info", "stack_info", "extra"]
    result = {}
    for valid_argument in valid_arguments:
        if kwargs.get(valid_argument):
            result[valid_argument] = kwargs.get(valid_argument)
    return result


@add_logger
def log_api_error(*args, logger: Logger=None, **kwargs) -> None:
    # TODO find some sensible things to do here
    pass


@add_logger
def log_missing_response(*args, logger: Logger=None, **kwargs) -> None:
    pass


@add_logger
def log_exception(*args, logger: Logger=None, **kwargs) -> None:
    stripped_kwargs = strip_log_kwargs(kwargs)
    logger.exception(*args, **stripped_kwargs)




def log_info(*args, **kwargs):
    pass