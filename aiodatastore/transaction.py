from typing import Dict

__all__ = (
    "ReadOnlyOptions",
    "ReadWriteOptions",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/beginTransaction#ReadOnly
class ReadOnlyOptions:
    def to_ds(self) -> Dict[str, Dict]:
        return {"readOnly": {}}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/beginTransaction#ReadWrite
class ReadWriteOptions:
    __slots__ = ("previous_transaction",)

    def __init__(self, previous_transaction: str) -> None:
        self.previous_transaction = previous_transaction

    def to_ds(self) -> Dict[str, Dict[str, str]]:
        return {
            "readWrite": {
                "previousTransaction": self.previous_transaction,
            },
        }
