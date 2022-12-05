from typing import Any, Dict, List, Optional

from aiodatastore.decorators import dataclass
from aiodatastore.key import Key

__all__ = (
    "MutationResult",
    "CommitResult",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#MutationResult
@dataclass
class MutationResult:
    key: Optional[Key]
    version: str
    conflict_detected: bool = False

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "MutationResult":
        key = data.get("key")
        if key is not None:
            key = Key.from_ds(key)

        return cls(
            key=key,
            version=str(data["version"]),
            conflict_detected=bool(data.get("conflictDetected", False)),
        )


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#response-body
@dataclass
class CommitResult:
    mutation_results: List[MutationResult]
    index_updates: int

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "CommitResult":
        results = [MutationResult.from_ds(mr) for mr in data["mutationResults"]]

        return cls(
            mutation_results=results,
            index_updates=int(data.get("indexUpdates", 0)),
        )
