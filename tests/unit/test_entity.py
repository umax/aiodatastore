import unittest

from aiodatastore import (
    Entity,
    EntityResult,
    Key,
    PartitionId,
    PathElement,
    StringValue,
)


class TestEntity(unittest.TestCase):
    def setUp(self):
        self.key = Key(PartitionId("project1"), [PathElement("kind1")])

    def test__init(self):
        e = Entity(None, {})
        assert e.key is None
        assert e.properties == {}

    def test__from_ds__no_key(self):
        e = Entity.from_ds(
            {
                "key": None,
                "properties": {},
            }
        )
        assert isinstance(e, Entity)
        assert e.key is None
        assert e.properties == {}

    def test__from_ds__no_properties(self):
        e = Entity.from_ds(
            {
                "key": self.key.to_ds(),
                "properties": {},
            }
        )
        assert isinstance(e, Entity)
        assert e.key == self.key
        assert e.properties == {}

    def test__from_ds__invalid_value(self):
        with self.assertRaises(RuntimeError):
            Entity.from_ds(
                {
                    "key": self.key.to_ds(),
                    "properties": {
                        "field1": {
                            "key": "value",
                        }
                    },
                }
            )

    def test__from_ds(self):
        e = Entity.from_ds(
            {
                "key": self.key.to_ds(),
                "properties": {
                    "field1": {
                        "stringValue": "str1",
                        "excludeFromIndexes": False,
                    },
                },
            }
        )
        assert isinstance(e, Entity)
        assert e.key == self.key
        assert e.properties == {
            "field1": StringValue("str1", indexed=True),
        }

    def test__to_ds__no_key(self):
        e = Entity(None, {})
        assert e.to_ds() == {
            "key": None,
            "properties": {},
        }

    def test__to_ds__no_properties(self):
        e = Entity(self.key, {})
        assert e.to_ds() == {
            "key": self.key.to_ds(),
            "properties": {},
        }

    def test__to_ds(self):
        e = Entity(self.key, {"field1": StringValue("str1")})
        assert e.to_ds() == {
            "key": self.key.to_ds(),
            "properties": {
                "field1": StringValue("str1").to_ds(),
            },
        }


class TestEntityResult(unittest.TestCase):
    def setUp(self):
        key = Key(PartitionId("project1"), [PathElement("kind1")])
        self.entity = Entity(key, {})

    def test__init__default_params(self):
        er = EntityResult(self.entity)
        assert er.entity == self.entity
        assert er.version == ""
        assert er.cursor == ""

    def test__init__custom_params(self):
        er = EntityResult(self.entity, version="version1", cursor="cursor1")
        assert er.entity == self.entity
        assert er.version == "version1"
        assert er.cursor == "cursor1"

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
