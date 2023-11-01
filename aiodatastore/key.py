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
        validate_id: bool = True,
    ) -> None:
        self.kind = kind
        self.id = id
        self.name = name

        if validate_id and self.id:
            self._validate_id()

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, PathElement)
            and self.kind == other.kind
            and self.id == other.id
            and self.name == other.name
        )

    def _validate_id(self):
        try:
            int(self.id)
        except ValueError:
            raise ValueError(
                f"`id` value of PathElement should follow int64 format: {self.id}"
            )

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "PathElement":
        return cls(
            kind=data["kind"],
            id=data.get("id"),
            name=data.get("name"),
            validate_id=False,
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

    def __init__(
        self,
        partition_id: PartitionId,
        path: List[PathElement],
        validate_path: bool = True,
    ) -> None:
        self.partition_id = partition_id
        self.path = path

        if validate_path:
            self._validate_path()

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Key)
            and self.partition_id == other.partition_id
            and self.path == other.path
        )

    def _validate_path(self):
        if not self.path:
            raise ValueError("`path` value of Key can never be empty")

        if len(self.path) > 100:
            raise ValueError("`path` value of Key can have at most 100 elements")

    @classmethod
    def from_ds(cls, data: Dict[str, Any]) -> "Key":
        return cls(
            partition_id=PartitionId.from_ds(data["partitionId"]),
            path=[PathElement.from_ds(path) for path in data["path"]],
            validate_path=False,
        )

    def to_ds(self) -> Dict[str, Any]:
        return {
            "partitionId": self.partition_id.to_ds(),
            "path": [path.to_ds() for path in self.path],
        }
