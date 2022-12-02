import unittest

from aiodatastore.property import PropertyReference
from aiodatastore.query import Projection, PropertyOrder, Query


class TestProjection(unittest.TestCase):
    def test__from_ds(self):
        p = Projection.from_ds({
            "property": {
                "name": "property1",
            },
        })
        assert isinstance(p, Projection)
        assert p.property == PropertyReference("property1")

    def test__to_ds(self):
        p = Projection(PropertyReference("property1"))
        assert p.to_ds() == {
            "property": {
                "name": "property1",
            },
        }

    def test_init(self):
        p = Projection(PropertyReference("property1"))
        assert p.property == PropertyReference("property1")
