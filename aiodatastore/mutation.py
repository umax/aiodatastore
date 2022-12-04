from typing import Any, Dict

from aiodatastore.constants import Operation
from aiodatastore.decorators import dataclass
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
    def to_ds(self) -> Dict[str, Any]:
        return {self.operation.value: self.entity.to_ds()}


@dataclass
class InsertMutation(Mutation):
    entity: Entity
    operation: Operation = Operation.INSERT


@dataclass
class UpdateMutation(Mutation):
    entity: Entity
    operation: Operation = Operation.UPDATE


@dataclass
class UpsertMutation(Mutation):
    entity: Entity
    operation: Operation = Operation.UPSERT


@dataclass
class DeleteMutation(Mutation):
    key: Key
    operation: Operation = Operation.DELETE

    def to_ds(self) -> Dict[str, Any]:
        return {self.operation.value: self.key.to_ds()}
