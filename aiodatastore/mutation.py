from typing import Any, Dict

from aiodatastore.constants import Operation
from aiodatastore.entity import Entity
from aiodatastore.key import Key

__all__ = (
    "Mutation",
    "InsertMutation",
    "UpdateMutation",
    "UpsertMutation",
    "DeleteMutation",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#Mutation
class Mutation:
    __slots__ = ("entity",)
    operation: Operation

    def __init__(self, entity: Entity) -> None:
        self.entity = entity

    def to_ds(self) -> Dict[str, Any]:
        return {self.operation.value: self.entity.to_ds()}


class InsertMutation(Mutation):
    operation: Operation = Operation.INSERT


class UpdateMutation(Mutation):
    operation: Operation = Operation.UPDATE


class UpsertMutation(Mutation):
    operation: Operation = Operation.UPSERT


class DeleteMutation:
    __slots__ = ("key",)
    operation: Operation = Operation.DELETE

    def __init__(self, key: Key) -> None:
        self.key = key

    def to_ds(self) -> Dict[str, Any]:
        return {self.operation.value: self.key.to_ds()}
