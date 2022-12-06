import unittest

from aiodatastore import Entity, EntityResult, Key, PartitionId, PathElement


class TestEntityResult(unittest.TestCase):
    def setUp(self):
        key = Key(PartitionId("project1"), [PathElement("kind1")])
        self.entity = Entity(key, {})

    def test__from_ds(self):
        er = EntityResult.from_ds(
            {
                "entity": self.entity.to_ds(),
            }
        )
        assert isinstance(er, EntityResult)
        assert er.entity == self.entity
        assert er.version == ""
        assert er.cursor == ""

        er = EntityResult.from_ds(
            {
                "entity": self.entity.to_ds(),
                "version": "version1",
                "cursor": "cursor1",
            }
        )
        assert er.version == "version1"
        assert er.cursor == "cursor1"

    def test__to_ds(self):
        er = EntityResult(self.entity)
        assert er.to_ds() == {
            "entity": self.entity.to_ds(),
            "version": "",
            "cursor": "",
        }

        er = EntityResult(self.entity, version="version1", cursor="cursor1")
        assert er.to_ds() == {
            "entity": self.entity.to_ds(),
            "version": "version1",
            "cursor": "cursor1",
        }

    def test_init(self):
        er = EntityResult(self.entity)
        assert er.entity == self.entity
        assert er.version == ""
        assert er.cursor == ""

        er = EntityResult(self.entity, version="version1", cursor="cursor1")
        assert er.entity == self.entity
        assert er.version == "version1"
        assert er.cursor == "cursor1"
