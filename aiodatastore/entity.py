from typing import Any, Dict, Optional

from aiodatastore.decorators import dataclass
from aiodatastore.key import Key
from aiodatastore.values import VALUE_TYPES, Value

__all__ = (
    "Entity",
    "EntityResult",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#Entity
@dataclass
class Entity:
    key: Optional[Key]
    properties: Dict[str, Value]

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
            properties[prop_name] = value_type(
                raw_value=prop_value[key],
                indexed=not prop_value.get("excludeFromIndexes"),
            )

        return cls(
            key=Key.from_ds(data.get("key")),
            properties=properties,
        )

    def to_ds(self) -> Dict[str, Any]:
        return {
            "key": self.key.to_ds() if self.key else None,
            "properties": {k: v.to_ds() for k, v in self.properties.items()},
        }


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/EntityResult
@dataclass
class EntityResult:
    entity: Entity
    version: str = ""
    cursor: str = ""

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "EntityResult":
        return cls(
            entity=Entity.from_ds(data["entity"]),
            version=data.get("version", ""),
            cursor=data.get("cursor", ""),
        )

    def to_ds(self) -> Dict[str, Any]:
        return {
            "entity": self.entity.to_ds(),
            "version": self.version,
            "cursor": self.cursor,
        }
