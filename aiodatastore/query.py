from typing import Any, Dict, List, Optional, Union

from aiodatastore.constants import MoreResultsType, ResultType
from aiodatastore.decorators import dataclass
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
@dataclass
class Projection:
    property: PropertyReference

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "Projection":
        return cls(property=PropertyReference(data["property"]["name"]))

    def to_ds(self) -> Dict[str, Any]:
        return {"property": self.property.to_ds()}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#KindExpression
@dataclass
class KindExpression:
    name: str

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "KindExpression":
        return cls(name=data["name"])

    def to_ds(self) -> Dict[str, Any]:
        return {"name": self.name}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#Query
@dataclass
class Query:
    projection: Optional[List[Projection]] = None
    kind: Optional[KindExpression] = None
    filter: Optional[Union[CompositeFilter, PropertyFilter]] = None
    order: Optional[List[PropertyOrder]] = None
    distinct_on: Optional[List[PropertyReference]] = None
    start_cursor: str = ""
    end_cursor: str = ""
    offset: Optional[int] = None
    limit: Optional[int] = None

    def to_ds(self) -> Dict[str, Any]:
        data = {"kind": []}

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
@dataclass
class GqlQueryParameter:
    value: Optional[Value] = None
    cursor: Optional[str] = None

    def to_ds(self) -> Dict[str, Any]:
        if self.cursor is not None:
            return {"cursor": self.cursor}

        return {"value": self.value.to_ds()}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#GqlQuery
@dataclass
class GQLQuery:
    query: str
    allow_literals: bool = True
    named_bindings: Optional[Dict[str, GqlQueryParameter]] = None
    positional_bindings: Optional[List[Any]] = None

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
@dataclass
class QueryResultBatch:
    entity_results: List[EntityResult]
    entity_result_type: ResultType = ResultType.UNSPECIFIED
    skipped_results: int = 0
    skipped_cursor: Optional[str] = None
    end_cursor: str = ""
    more_results: MoreResultsType = MoreResultsType.UNSPECIFIED
    snapshot_version: str = ""

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "QueryResultBatch":
        results = [EntityResult.from_ds(er) for er in data.get("entityResults", [])]

        return cls(
            entity_results=results,
            entity_result_type=ResultType(data["entityResultType"]),
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
