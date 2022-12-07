from typing import Any, Dict, Optional

from aiodatastore.key import Key
from aiodatastore.values import VALUE_TYPES, NullValue

__all__ = (
    "Entity",
    "EntityResult",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#Entity
class Entity:
    __slots__ = ("key", "properties")

    def __init__(self, key: Optional[Key], properties: Dict[str, Any]) -> None:
        self.key = key
        self.properties = properties

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Entity)
            and self.key == other.key
            and self.properties == other.properties
        )

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "Entity":
        properties = {}
        for prop_name, prop_value in data.get("properties", {}).items():
            for key in prop_value:
                if key.endswith("Value"):
                    break
            else:
                raise RuntimeError(
                    f'unsupported value of "{prop_name}" property: {prop_value}'
                )

            value_type = VALUE_TYPES[key]
            if value_type is NullValue:
                _value = NullValue(indexed=not prop_value.get("excludeFromIndexes"))
            else:
                _value = value_type(
                    None,
                    raw_value=prop_value[key],
                    indexed=not prop_value.get("excludeFromIndexes"),
                )
            properties[prop_name] = _value

        key = data.get("key")
        if key is not None:
            key = Key.from_ds(key)

        return cls(key, properties=properties)

    def to_ds(self) -> Dict[str, Any]:
        return {
            "key": self.key.to_ds() if self.key else None,
            "properties": {k: v.to_ds() for k, v in self.properties.items()},
        }


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/EntityResult
class EntityResult:
    __slots__ = ("entity", "version", "cursor")

    def __init__(self, entity: Entity, version: str = "", cursor: str = "") -> None:
        self.entity = entity
        self.version = version
        self.cursor = cursor

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, EntityResult)
            and self.entity == other.entity
            and self.version == other.version
            and self.cursor == other.cursor
        )

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "EntityResult":
        return cls(
            Entity.from_ds(data["entity"]),
            version=data.get("version", ""),
            cursor=data.get("cursor", ""),
        )

    def to_ds(self) -> Dict[str, Any]:
        return {
            "entity": self.entity.to_ds(),
            "version": self.version,
            "cursor": self.cursor,
        }
