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

    def test__getitem__(self):
        entity = Entity(None, {"field1": StringValue("str1")})
        assert entity["field1"] == entity.properties["field1"]

        with self.assertRaises(KeyError):
            entity["some-key123"]

    def test__setitem__(self):
        entity = Entity(None, {})
        entity["field2"] = StringValue("str2")
        assert entity["field2"] == entity.properties["field2"]

    def test__delitem__(self):
        entity = Entity(None, {"field3": StringValue("str3")})
        assert len(entity.properties) == 1
        del entity["field3"]
        assert len(entity.properties) == 0
        assert "field3" not in entity.properties

    def test_eq(self):
        key1 = Key(PartitionId("project1"), [PathElement("kind1")])
        key2 = Key(PartitionId("project1"), [PathElement("kind1")])
        key3 = Key(PartitionId("project2"), [PathElement("kind1")])
        assert Entity(key1, {}) == Entity(key2, {})
        assert Entity(key1, {}) != Entity(key3, {})

        properties1 = {}
        properties2 = {"field1": StringValue("str1")}
        assert Entity(key1, properties1) != Entity(key2, properties2)

        properties1 = {"field1": StringValue("v1")}
        properties2 = {"field1": StringValue("v1")}
        assert Entity(key1, properties1) == Entity(key2, properties2)

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
