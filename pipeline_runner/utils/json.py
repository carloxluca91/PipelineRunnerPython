import errno
import json
import logging
import os
import re

from typing import Dict


class JsonUtils:

    _logger = logging.getLogger(__name__)

    @classmethod
    def load_json(cls, json_file_path: str) -> Dict:

        if os.path.exists(json_file_path):

            cls._logger.info(f"File '{json_file_path}' exists")
            with open(json_file_path, mode="r", encoding="UTF-8") as f:
                return json.load(f)

        else:
            raise FileNotFoundError(errno.ENOENT,
                                    os.strerror(errno.ENOENT),
                                    json_file_path)

    @classmethod
    def from_java_to_python_convention(cls, dict_: Dict) -> Dict:

        python_convention_dict_ = {}
        for key in dict_:

            python_convention_key = re.sub(r"([A-Z])", "_\\1", key, 0).lower()
            python_convention_dict_[python_convention_key] = dict_[key]

        return python_convention_dict_
