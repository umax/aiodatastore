import unittest

from aiodatastore import (
    CompositeFilter,
    CompositeFilterOperator,
    IntegerValue,
    PropertyFilter,
    PropertyFilterOperator,
    PropertyReference,
)


class TestCompositeFilter(unittest.TestCase):
    def test__init(self):
        prop_filter = PropertyFilter(
            property=PropertyReference("prop1"),
            op=PropertyFilterOperator.EQUAL,
            value=IntegerValue(value=123),
        )
        composite_filter = CompositeFilter(
            op=CompositeFilterOperator.AND,
            filters=[],
        )
        f = CompositeFilter(
            op=CompositeFilterOperator.AND,
            filters=[prop_filter, composite_filter],
        )
        assert f.op == CompositeFilterOperator.AND
        assert f.filters == [prop_filter, composite_filter]

    def test__to_ds(self):
        prop_filter = PropertyFilter(
            property=PropertyReference("prop1"),
            op=PropertyFilterOperator.EQUAL,
            value=IntegerValue(value=123),
        )
        composite_filter = CompositeFilter(
            op=CompositeFilterOperator.AND,
            filters=[],
        )
        f = CompositeFilter(
            op=CompositeFilterOperator.AND,
            filters=[prop_filter, composite_filter],
        )
        assert f.to_ds() == {
            "compositeFilter": {
                "op": CompositeFilterOperator.AND.value,
                "filters": [
                    prop_filter.to_ds(),
                    composite_filter.to_ds(),
                ],
            },
        }


class TestPropertyFilter(unittest.TestCase):
    def test__init(self):
        f = PropertyFilter(
            property=PropertyReference("prop1"),
            op=PropertyFilterOperator.EQUAL,
            value=IntegerValue(value=123),
        )
        assert f.property == PropertyReference("prop1")
        assert f.op == PropertyFilterOperator.EQUAL
        assert f.value.value == 123

    def test__to_ds(self):
        f = PropertyFilter(
            property=PropertyReference("prop1"),
            op=PropertyFilterOperator.EQUAL,
            value=IntegerValue(value=123),
        )
        assert f.to_ds() == {
            "propertyFilter": {
                "property": PropertyReference("prop1").to_ds(),
                "op": PropertyFilterOperator.EQUAL.value,
                "value": IntegerValue(value=123).to_ds(),
            },
        }
