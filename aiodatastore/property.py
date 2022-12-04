from typing import Any, Dict

from aiodatastore.constants import Direction
from aiodatastore.decorators import dataclass

__all__ = (
    "PropertyReference",
    "PropertyOrder",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#PropertyReference
@dataclass
class PropertyReference:
    name: str

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "PropertyReference":
        return cls(name=data["name"])

    def to_ds(self) -> Dict[str, Any]:
        return {"name": self.name}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#PropertyOrder
@dataclass
class PropertyOrder:
    property: PropertyReference
    direction: Direction = Direction.ASCENDING

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "PropertyOrder":
        return cls(
            property=PropertyReference(data["property"]["name"]),
            direction=Direction(data["direction"]),
        )

    def to_ds(self) -> Dict[str, Any]:
        return {
            "property": self.property.to_ds(),
            "direction": self.direction.value,
        }
