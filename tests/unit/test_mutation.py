import unittest

from aiodatastore import (
    Entity,
    Key,
    PartitionId,
    PathElement,
    InsertMutation,
    UpdateMutation,
    UpsertMutation,
    DeleteMutation,
)


class SetupMixin:
    def setUp(self):
        self.key = key = Key(PartitionId("project1"), [PathElement("kind1")])
        self.entity = Entity(key, {})


class TestInsertMutation(SetupMixin, unittest.TestCase):
    def test__init(self):
        mutation = InsertMutation(self.entity)
        assert mutation.entity == self.entity

    def test__to_ds(self):
        mutation = InsertMutation(self.entity)
        assert mutation.to_ds() == {"insert": self.entity.to_ds()}


class TestUpdateMutation(SetupMixin, unittest.TestCase):
    def test__init(self):
        mutation = UpdateMutation(self.entity)
        assert mutation.entity == self.entity

    def test__to_ds(self):
        mutation = UpdateMutation(self.entity)
        assert mutation.to_ds() == {"update": self.entity.to_ds()}


class TestUpsertMutation(SetupMixin, unittest.TestCase):
    def test__init(self):
        mutation = UpsertMutation(self.entity)
        assert mutation.entity == self.entity

    def test__to_ds(self):
        mutation = UpsertMutation(self.entity)
        assert mutation.to_ds() == {"upsert": self.entity.to_ds()}


class TestDeleteMutation(SetupMixin, unittest.TestCase):
    def test__init(self):
        mutation = DeleteMutation(self.key)
        assert mutation.key == self.key

    def test__to_ds(self):
        mutation = DeleteMutation(self.key)
        assert mutation.to_ds() == {"delete": self.key.to_ds()}
