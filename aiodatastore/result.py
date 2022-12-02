from typing import Any, Dict, List, Optional

from aiodatastore.constants import MoreResultsType, ResultType
from aiodatastore.decorators import dataclass
from aiodatastore.entity import EntityResult

__all__ = ("QueryResultBatch"),


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#QueryResultBatch
@dataclass
class QueryResultBatch:
    entity_results: List[EntityResult]
    entity_result_type: ResultType = (ResultType.UNSPECIFIED,)
    skipped_results: int = (0,)
    skipped_cursor: Optional[str] = (None,)
    end_cursor: str = ("",)
    more_results: MoreResultsType = (MoreResultsType.UNSPECIFIED,)
    snapshot_version: str = ("",)

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "QueryResultBatch":
        results = [EntityResult.from_ds(er) for er in data.get("entityResults", [])]
        return cls(
            results,
            ResultType(data["entityResultType"]),
            skipped_results=data.get("skippedResults", 0),
            skipped_cursor=data.get("skippedCursor"),
            end_cursor=data["endCursor"],
            more_results=MoreResultsType(data["moreResults"]),
            snapshot_version=data.get("snapshotVersion", ""),
        )

    def to_ds(self) -> Dict[str, Any]:
        data = {
            "endCursor": self.end_cursor,
            "entityResults": [er.to_ds() for er in self.entity_results],
            "entityResultType": self.entity_result_type.value,
            "moreResults": self.more_results.value,
            "snapshotVersion": self.snapshot_version,
        }
        if self.skipped_results is not None:
            data["skippedResults"] = self.skipped_results
        if self.skipped_cursor is not None:
            data["skippedCursor"] = self.skipped_cursor

        return data
