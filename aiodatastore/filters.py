from typing import Any, Dict, List

from aiodatastore.constants import (
    CompositeFilterOperator,
    PropertyFilterOperator,
)
from aiodatastore.property import PropertyReference
from aiodatastore.values import Value

__all__ = (
    "Filter",
    "CompositeFilter",
    "PropertyFilter",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#Filter
class Filter:
    def to_ds(self) -> Dict[str, Any]:
        raise NotImplementedError


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#PropertyFilter
class PropertyFilter(Filter):
    __slots__ = ("property", "op", "value")

    def __init__(
        self,
        property: PropertyReference,
        op: PropertyFilterOperator,
        value: Value,
    ) -> None:
        self.property = property
        self.op = op
        self.value = value

    def to_ds(self) -> Dict[str, Any]:
        return {
            "propertyFilter": {
                "property": self.property.to_ds(),
                "op": self.op.value,
                "value": self.value.to_ds(),
            },
        }


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#CompositeFilter
class CompositeFilter(Filter):
    __slots__ = ("op", "filters")

    def __init__(self, op: CompositeFilterOperator, filters: List[Filter]) -> None:
        self.op = op
        self.filters = filters

    def to_ds(self) -> Dict[str, Any]:
        return {
            "compositeFilter": {
                "op": self.op.value,
                "filters": [f.to_ds() for f in self.filters],
            },
        }
