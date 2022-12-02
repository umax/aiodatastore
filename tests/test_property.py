import unittest

from aiodatastore.constants import Direction
from aiodatastore.property import PropertyReference, PropertyOrder


class TestPropertyReference(unittest.TestCase):
    def test__from_ds(self):
        pr = PropertyReference.from_ds({"name": "name1"})
        assert isinstance(pr, PropertyReference)
        assert pr.name == "name1"

    def test__to_ds(self):
        pr = PropertyReference("name1")
        pr.to_ds() == {"name": "name1"}

    def test_init(self):
        pr = PropertyReference("name1")
        assert pr.name == "name1"


class TestPropertyOrder(unittest.TestCase):
    def test__from_ds(self):
        po = PropertyOrder.from_ds(
            {
                "property": {
                    "name": "property1",
                },
                "direction": "DESCENDING",
            }
        )
        assert isinstance(po, PropertyOrder)
        assert po.property == PropertyReference("property1")
        assert po.direction == Direction.DESCENDING

    def test__to_ds(self):
        po = PropertyOrder(
            property=PropertyReference("property1"),
            direction=Direction.DESCENDING,
        )
        assert po.to_ds() == {
            "property": {
                "name": "property1",
            },
            "direction": "DESCENDING",
        }

    def test_init(self):
        po = PropertyOrder(
            property=PropertyReference("property1"),
        )
        assert po.property == PropertyReference("property1")
        assert po.direction == Direction.ASCENDING

        po = PropertyOrder(
            property=PropertyReference("property1"),
            direction=Direction.DESCENDING,
        )
        assert po.property == PropertyReference("property1")
        assert po.direction == Direction.DESCENDING
