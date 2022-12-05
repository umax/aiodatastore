from typing import Any, Dict, List, Optional

__all__ = (
    "PartitionId",
    "PathElement",
    "Key",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#PartitionId
class PartitionId:
    __slots__ = ("project_id", "namespace_id")

    def __init__(self, project_id: str, namespace_id: Optional[str] = None) -> None:
        self.project_id = project_id
        self.namespace_id = namespace_id

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, PartitionId)
            and self.project_id == other.project_id
            and self.namespace_id == other.namespace_id
        )

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "PartitionId":
        return cls(data["projectId"], namespace_id=data.get("namespaceId"))

    def to_ds(self) -> Dict[str, str]:
        data = {"projectId": self.project_id}
        if self.namespace_id is not None:
            data["namespaceId"] = self.namespace_id

        return data


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#pathelement
class PathElement:
    __slots__ = ("kind", "id", "name")

    def __init__(
        self,
        kind: str,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> None:
        self.kind = kind
        self.id = id
        self.name = name

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, PathElement)
            and self.kind == other.kind
            and self.id == other.id
            and self.name == other.name
        )

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
class Key:
    __slots__ = ("partition_id", "path")

    def __init__(self, partition_id: PartitionId, path: List[PathElement]) -> None:
        self.partition_id = partition_id
        self.path = path

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Key)
            and self.partition_id == other.partition_id
            and self.path == other.path
        )

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
