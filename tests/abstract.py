import configparser
import logging

from abc import ABC
from logging import config
from unittest import TestCase
from tests.paths import LOGGING_INI_FILE, SPARK_JOB_INI_FILE


class AbstractTest(TestCase, ABC):

    @classmethod
    def setUpClass(cls) -> None:

        # Logging configuration
        with open(LOGGING_INI_FILE, "r", encoding="UTF-8") as f:
            config.fileConfig(f)

        cls._logger = logging.getLogger(__name__)
        cls._logger.info("Successfully loaded logging configuration")

        # Job properties
        cls._job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        with open(SPARK_JOB_INI_FILE, mode="r", encoding="UTF-8") as f:

            cls._job_properties.read_file(f)
            cls._logger.info(f"Successfully loaded job properties dict")
