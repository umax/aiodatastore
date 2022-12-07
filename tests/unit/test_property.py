import unittest

from aiodatastore.constants import Direction
from aiodatastore.property import PropertyOrder, PropertyReference


class TestPropertyReference(unittest.TestCase):
    def test__init(self):
        pr = PropertyReference("name1")
        assert pr.name == "name1"

    def test__eq(self):
        assert PropertyReference("name1") == PropertyReference("name1")
        assert PropertyReference("name1") != PropertyReference("name2")

    def test__from_ds(self):
        pr = PropertyReference.from_ds({"name": "name1"})
        assert isinstance(pr, PropertyReference)
        assert pr.name == "name1"

    def test__to_ds(self):
        pr = PropertyReference("name1")
        pr.to_ds() == {"name": "name1"}


class TestPropertyOrder(unittest.TestCase):
    def test__init__default_params(self):
        po = PropertyOrder(PropertyReference("property1"))
        assert po.property == PropertyReference("property1")
        assert po.direction == Direction.ASCENDING

    def test__init__custom_params(self):
        po = PropertyOrder(
            PropertyReference("property1"),
            direction=Direction.DESCENDING,
        )
        assert po.property == PropertyReference("property1")
        assert po.direction == Direction.DESCENDING

    def test__eq(self):
        po1 = PropertyOrder(PropertyReference("name1"))
        po2 = PropertyOrder(PropertyReference("name1"))
        assert po1 == po2

        po1 = PropertyOrder(
            PropertyReference("name1"),
            direction=Direction.ASCENDING,
        )
        po2 = PropertyOrder(
            PropertyReference("name1"),
            direction=Direction.DESCENDING,
        )
        assert po1 != po2

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
            PropertyReference("property1"),
            direction=Direction.DESCENDING,
        )
        assert po.to_ds() == {
            "property": {
                "name": "property1",
            },
            "direction": "DESCENDING",
        }
