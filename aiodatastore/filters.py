from typing import Any, Dict, List

from aiodatastore.constants import CompositeFilterOperator, PropertyFilterOperator
from aiodatastore.decorators import dataclass
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
@dataclass
class PropertyFilter(Filter):
    property: PropertyReference
    op: PropertyFilterOperator
    value: Value

    def to_ds(self) -> Dict[str, Any]:
        return {
            "propertyFilter": {
                "property": self.property.to_ds(),
                "op": self.op.value,
                "value": self.value.to_ds(),
            },
        }


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#CompositeFilter
@dataclass
class CompositeFilter(Filter):
    op: CompositeFilterOperator
    filters: List[Filter]

    def to_ds(self) -> Dict[str, Any]:
        return {
            "compositeFilter": {
                "op": self.op.value,
                "filters": [f.to_ds() for f in self.filters],
            },
        }
