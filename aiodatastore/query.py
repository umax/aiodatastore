from typing import Any, Dict, List, Optional, Union

from aiodatastore.constants import MoreResultsType, ResultType
from aiodatastore.entity import EntityResult
from aiodatastore.filters import CompositeFilter, PropertyFilter
from aiodatastore.property import PropertyReference, PropertyOrder
from aiodatastore.values import Value

__all__ = (
    "Projection",
    "KindExpression",
    "Query",
    "GqlQueryParameter",
    "GQLQuery",
    "QueryResultBatch",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#Projection
class Projection:
    __slots__ = ("property",)

    def __init__(self, property: PropertyReference) -> None:
        self.property = property

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Projection) and self.property == other.property

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "Projection":
        return cls(PropertyReference(data["property"]["name"]))

    def to_ds(self) -> Dict[str, Any]:
        return {"property": self.property.to_ds()}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#KindExpression
class KindExpression:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, KindExpression) and self.name == other.name

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "KindExpression":
        return cls(data["name"])

    def to_ds(self) -> Dict[str, str]:
        return {"name": self.name}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#Query
class Query:
    __slots__ = (
        "projection",
        "kind",
        "filter",
        "order",
        "distinct_on",
        "start_cursor",
        "end_cursor",
        "offset",
        "limit",
    )

    def __init__(
        self,
        projection: Optional[List[Projection]] = None,
        kind: Optional[KindExpression] = None,
        filter: Optional[Union[CompositeFilter, PropertyFilter]] = None,
        order: Optional[List[PropertyOrder]] = None,
        distinct_on: Optional[List[PropertyReference]] = None,
        start_cursor: str = "",
        end_cursor: str = "",
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> None:
        self.projection = projection
        self.kind = kind
        self.filter = filter
        self.order = order
        self.distinct_on = distinct_on
        self.start_cursor = start_cursor
        self.end_cursor = end_cursor
        self.offset = offset
        self.limit = limit

    def to_ds(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"kind": []}

        if self.projection:
            data["projection"] = [p.to_ds() for p in self.projection]
        if self.kind:
            data["kind"] = [self.kind.to_ds()]
        if self.filter:
            data["filter"] = self.filter.to_ds()
        if self.order:
            data["order"] = [o.to_ds() for o in self.order]
        if self.distinct_on:
            data["distinctOn"] = [d.to_ds() for d in self.distinct_on]
        if self.start_cursor:
            data["startCursor"] = self.start_cursor
        if self.end_cursor:
            data["endCursor"] = self.end_cursor
        if self.offset is not None:
            data["offset"] = int(self.offset)
        if self.limit is not None:
            data["limit"] = int(self.limit)

        return data


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/GqlQueryParameter
class GqlQueryParameter:
    __slots__ = ("value", "cursor")

    def __init__(
        self,
        value: Optional[Value] = None,
        cursor: Optional[str] = None,
    ) -> None:
        self.value = value
        self.cursor = cursor
        if value is None and cursor is None:
            raise RuntimeError("value or cursor should be provided")

    def to_ds(self) -> Dict[str, Any]:
        if self.cursor is not None:
            return {"cursor": self.cursor}
        if self.value is not None:
            return {"value": self.value.to_ds()}
        return {}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#GqlQuery
class GQLQuery:
    __slots__ = ("query", "allow_literals", "named_bindings", "positional_bindings")

    def __init__(
        self,
        query: str,
        allow_literals: bool = True,
        named_bindings: Optional[Dict[str, GqlQueryParameter]] = None,
        positional_bindings: Optional[List[Any]] = None,
    ) -> None:
        self.query = query
        self.allow_literals = allow_literals
        self.named_bindings = named_bindings
        self.positional_bindings = positional_bindings

    def to_ds(self) -> Dict[str, Any]:
        return {
            "queryString": self.query,
            "allowLiterals": self.allow_literals,
            "namedBindings": {
                key: value.to_ds() for key, value in (self.named_bindings or {}).items()
            },
            "positionalBindings": [v.to_ds() for v in (self.positional_bindings or [])],
        }


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#QueryResultBatch
class QueryResultBatch:
    __slots__ = (
        "entity_results",
        "entity_result_type",
        "skipped_results",
        "skipped_cursor",
        "end_cursor",
        "more_results",
        "snapshot_version",
    )

    def __init__(
        self,
        skipped_results: int = 0,
        skipped_cursor: Optional[str] = None,
        entity_result_type: ResultType = ResultType.UNSPECIFIED,
        entity_results: Optional[List[EntityResult]] = None,
        end_cursor: str = "",
        more_results: MoreResultsType = MoreResultsType.UNSPECIFIED,
        snapshot_version: str = "",
    ) -> None:
        self.skipped_results = skipped_results
        self.skipped_cursor = skipped_cursor
        self.entity_result_type = entity_result_type
        self.entity_results = entity_results or []
        self.end_cursor = end_cursor
        self.more_results = more_results
        self.snapshot_version = snapshot_version

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "QueryResultBatch":
        return cls(
            skipped_results=int(data.get("skippedResults", 0)),
            skipped_cursor=data.get("skippedCursor"),
            entity_result_type=ResultType(data["entityResultType"]),
            entity_results=[
                EntityResult.from_ds(er) for er in data.get("entityResults", [])
            ],
            end_cursor=data["endCursor"],
            more_results=MoreResultsType(data["moreResults"]),
            snapshot_version=data.get("snapshotVersion", ""),
        )

    def to_ds(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "skippedResults": self.skipped_results,
            "entityResultType": self.entity_result_type.value,
            "entityResults": [er.to_ds() for er in self.entity_results],
            "endCursor": self.end_cursor,
            "moreResults": self.more_results.value,
            "snapshotVersion": self.snapshot_version,
        }
        if self.skipped_cursor is not None:
            data["skippedCursor"] = self.skipped_cursor

        return data
