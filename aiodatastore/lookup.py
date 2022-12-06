from typing import Any, Dict, List

from aiodatastore.entity import EntityResult
from aiodatastore.key import Key

__all__ = ("LookupResult",)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/lookup#response-body
class LookupResult:
    __slots__ = ("found", "missing", "deferred")

    def __init__(
        self,
        found: List[EntityResult],
        missing: List[EntityResult],
        deferred: List[Key],
    ) -> None:
        self.found = found
        self.missing = missing
        self.deferred = deferred

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "LookupResult":
        return cls(
            found=[EntityResult.from_ds(f) for f in data["found"]],
            missing=[EntityResult.from_ds(m) for m in data["missing"]],
            deferred=[Key.from_ds(d) for d in data["deferred"]],
        )
