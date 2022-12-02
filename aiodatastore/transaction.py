from typing import Dict
from aiodatastore.decorators import dataclass

__all__ = (
    "ReadOnlyOptions",
    "ReadWriteOptions",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/beginTransaction#ReadOnly
@dataclass
class ReadOnlyOptions:
    def to_ds(self) -> Dict[str, Dict]:
        return {"readOnly": {}}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/beginTransaction#ReadWrite
@dataclass
class ReadWriteOptions:
    previous_transaction: str

    def to_ds(self) -> Dict[str, Dict[str, str]]:
        return {
            "readWrite": {
                "previousTransaction": self.previous_transaction,
            },
        }
