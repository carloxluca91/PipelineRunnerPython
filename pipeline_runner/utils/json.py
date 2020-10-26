import errno
import json
import os
import re

from typing import Dict


def load_json(json_file_path: str) -> dict:

    if os.path.exists(json_file_path):
        with open(json_file_path, mode="r", encoding="UTF-8") as f:
            return json.load(f)
    else:
        raise FileNotFoundError(errno.ENOENT,
                                os.strerror(errno.ENOENT),
                                json_file_path)


def from_java_to_python_convention(dict_: Dict) -> Dict:

    python_convention_dict_ = {}
    for key in dict_:

        python_convention_key = re.sub(r"([A-Z])", "_\\1", key, 0).lower()
        python_convention_dict_[python_convention_key] = dict_[key]

    return python_convention_dict_
