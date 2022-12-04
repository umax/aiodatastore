from typing import Any, Dict

from aiodatastore.constants import Operation
from aiodatastore.decorators import dataclass
from aiodatastore.entity import Entity
from aiodatastore.key import Key

__all__ = (
    "Mutation",
    "DeleteMutation",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#Mutation
@dataclass
class Mutation:
    operation: Operation
    entity: Entity

    def to_ds(self) -> Dict[str, Any]:
        return {self.operation.value: self.entity.to_ds()}


@dataclass
class DeleteMutation:
    key: Key

    def to_ds(self) -> Dict[str, Any]:
        return {Operation.DELETE.value: self.key.to_ds()}
