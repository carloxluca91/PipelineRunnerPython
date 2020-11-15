from abc import ABC
from typing import Dict, Any, Tuple

from tests.abstract import AbstractTest


class ABCTestCreate(AbstractTest, ABC):

    _number_of_records = 100

    @staticmethod
    def build_test_dict(*tuples: Tuple[str, Any]) -> Dict[str, Any]:

        d = {}
        for tuple_ in tuples:
            d[tuple_[0]] = tuple_[1]
        return d

    def apply_test_for_dict(self, d: Dict[str, Any]):
        pass
