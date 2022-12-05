from typing import Any, Dict, List, Optional

from aiodatastore.key import Key

__all__ = (
    "MutationResult",
    "CommitResult",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#MutationResult
class MutationResult:
    __slots__ = ("version", "key", "conflict_detected")

    def __init__(
        self,
        version: str,
        key: Optional[Key] = None,
        conflict_detected: bool = False,
    ) -> None:
        self.version = version
        self.key = key
        self.conflict_detected = conflict_detected

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, MutationResult)
            and self.version == other.version
            and self.key == other.key
            and self.conflict_detected == other.conflict_detected
        )

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "MutationResult":
        key = data.get("key")
        if key is not None:
            key = Key.from_ds(key)

        return cls(
            version=str(data["version"]),
            key=key,
            conflict_detected=bool(data.get("conflictDetected", False)),
        )


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#response-body
class CommitResult:
    __slots__ = ("mutation_results", "index_updates")

    def __init__(
        self,
        mutation_results: List[MutationResult],
        index_updates: int,
    ) -> None:
        self.mutation_results = mutation_results
        self.index_updates = index_updates

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "CommitResult":
        results = [MutationResult.from_ds(mr) for mr in data["mutationResults"]]
        index = int(data.get("indexUpdates", 0))

        return cls(mutation_results=results, index_updates=index)
