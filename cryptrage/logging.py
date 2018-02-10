import logging
import logging.config
from logging import Logger  # type hinting
from os import path

import yaml


def setup_logging(*, config_path: str, name: str, default_level: int=logging.INFO) -> Logger:
    if config_path and path.exists(config_path):
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basisConfig(level=default_level)
    return logging.getLogger(name)