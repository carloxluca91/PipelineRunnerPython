from abc import ABC
from typing import Dict, Any

from tests.abstract import AbstractTest


class ABCTestMetadata(AbstractTest, ABC):

    _number_of_records = 100

    def apply_test_for_dict(self, d: Dict[str, Any]):
        pass
