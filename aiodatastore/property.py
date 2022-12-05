from typing import Any, Dict

from aiodatastore.constants import Direction

__all__ = (
    "PropertyReference",
    "PropertyOrder",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#PropertyReference
class PropertyReference:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PropertyReference) and self.name == other.name

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "PropertyReference":
        return cls(name=data["name"])

    def to_ds(self) -> Dict[str, str]:
        return {"name": self.name}


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#PropertyOrder
class PropertyOrder:
    __slots__ = ("property", "direction")

    def __init__(
        self,
        property: PropertyReference,
        direction: Direction = Direction.ASCENDING,
    ) -> None:
        self.property = property
        self.direction = direction

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, PropertyOrder)
            and self.property == other.property
            and self.direction == other.direction
        )

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
