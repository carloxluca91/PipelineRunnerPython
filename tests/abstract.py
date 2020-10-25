import os
import logging
import unittest
from logging import config

from pipeline_runner.pipeline.utils import load_json
from tests.paths import PROJECT_JSON_TEST_DIRECTORY, LOGGING_INI_FILE


class AbstractTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:

        with open(LOGGING_INI_FILE, mode="r", encoding="UTF-8") as f:
            config.fileConfig(f)

        cls._logger = logging.getLogger(__name__)

    def _load_json(self, json_file_name: str) -> dict:

        json_file_path: str = os.path.join(PROJECT_JSON_TEST_DIRECTORY, json_file_name)
        self._logger.info(f"Trying to load .json file at path '{json_file_path}'")
        json_content: dict = load_json(json_file_path)
        self._logger.info(f"Successfully loaded .json file at path '{json_file_path}'")

        return json_content
