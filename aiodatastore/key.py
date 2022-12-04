from typing import Any, Dict, List, Optional

from aiodatastore.decorators import dataclass

__all__ = (
    "PartitionId",
    "PathElement",
    "Key",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#PartitionId
@dataclass
class PartitionId:
    project_id: str
    namespace_id: Optional[str] = None

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "PartitionId":
        return cls(data["projectId"], namespace_id=data.get("namespaceId"))

    def to_ds(self) -> Dict[str, str]:
        data = {"projectId": self.project_id}
        if self.namespace_id is not None:
            data["namespaceId"] = self.namespace_id

        return data


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#pathelement
@dataclass
class PathElement:
    kind: str
    id: Optional[str] = None
    name: Optional[str] = None

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "PathElement":
        return cls(
            kind=data["kind"],
            id=data.get("id"),
            name=data.get("name"),
        )

    def to_ds(self) -> Dict[str, str]:
        data = {"kind": self.kind}
        if self.id is not None:
            data["id"] = self.id
        elif self.name is not None:
            data["name"] = self.name

        return data


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#Key
@dataclass
class Key:
    partition_id: PartitionId
    path: List[PathElement]

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "Key":
        return cls(
            partition_id=PartitionId.from_ds(data["partitionId"]),
            path=[PathElement.from_ds(path) for path in data["path"]],
        )

    def to_ds(self) -> Dict[str, Any]:
        return {
            "partitionId": self.partition_id.to_ds(),
            "path": [path.to_ds() for path in self.path],
        }
