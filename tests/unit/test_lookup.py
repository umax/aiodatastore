import unittest

from aiodatastore import (
    BooleanValue,
    Entity,
    EntityResult,
    Key,
    LookupResult,
    NullValue,
    PartitionId,
    PathElement,
)


class TestLookupResult(unittest.TestCase):
    def setUp(self):
        key1 = Key(PartitionId("project1"), [PathElement("kind1")])
        self.er1 = EntityResult(Entity(key1, {"a": NullValue()}))

        key2 = Key(PartitionId("project1"), [PathElement("kind1")])
        self.er2 = EntityResult(Entity(key2, {"b": BooleanValue(True)}))

        self.key3 = Key(PartitionId("project1"), [PathElement("kind1")])

    def test__init(self):
        lr = LookupResult([self.er1], [self.er2], [self.key3])
        assert lr.found == [self.er1]
        assert lr.missing == [self.er2]
        assert lr.deferred == [self.key3]

    def test__from_ds(self):
        lr = LookupResult.from_ds(
            {
                "found": [self.er1.to_ds()],
                "missing": [self.er2.to_ds()],
                "deferred": [self.key3.to_ds()],
            }
        )
        assert isinstance(lr, LookupResult)
