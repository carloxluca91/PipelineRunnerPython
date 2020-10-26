import errno
import json
import os


def load_json(json_file_path: str) -> dict:

    if os.path.exists(json_file_path):
        with open(json_file_path, mode="r", encoding="UTF-8") as f:
            return json.load(f)
    else:
        raise FileNotFoundError(errno.ENOENT,
                                os.strerror(errno.ENOENT),
                                json_file_path)
