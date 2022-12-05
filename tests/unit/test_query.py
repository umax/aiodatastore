import unittest

from aiodatastore.constants import Direction, PropertyFilterOperator
from aiodatastore.filters import PropertyFilter
from aiodatastore.property import PropertyOrder, PropertyReference
from aiodatastore.query import Projection, KindExpression, Query
from aiodatastore.values import IntegerValue


class TestProjection(unittest.TestCase):
    def test__from_ds(self):
        p = Projection.from_ds(
            {
                "property": {
                    "name": "property1",
                },
            }
        )
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


class TestKindExpression(unittest.TestCase):
    def test__from_ds(self):
        ke = KindExpression.from_ds({"name": "kind1"})
        assert isinstance(ke, KindExpression)
        assert ke.name == "kind1"

    def test__to_ds(self):
        ke = KindExpression("kind1")
        assert ke.to_ds() == {"name": "kind1"}

    def test_init(self):
        ke = KindExpression("kind1")
        assert ke.name == "kind1"


class TestQuery(unittest.TestCase):
    def test__to_ds__projection(self):
        q = Query()
        assert "projection" not in q.to_ds()

        q = Query(
            projection=[
                Projection(PropertyReference("property1")),
                Projection(PropertyReference("property2")),
            ]
        )
        assert q.to_ds()["projection"] == [
            {"property": {"name": "property1"}},
            {"property": {"name": "property2"}},
        ]

    def test__to_ds__kind(self):
        q = Query()
        assert q.to_ds()["kind"] == []

        kind = KindExpression("kind1")
        q = Query(kind=kind)
        assert q.to_ds()["kind"] == [kind.to_ds()]

    def test__to_ds__filter(self):
        q = Query()
        assert "filter" not in q.to_ds()

        filter1 = PropertyFilter(
            property=PropertyReference("property1"),
            op=PropertyFilterOperator.EQUAL,
            value=IntegerValue(value=123),
        )
        q = Query(filter=filter1)
        assert q.to_ds()["filter"] == filter1.to_ds()

    def test__to_ds__order(self):
        q = Query()
        assert "order" not in q.to_ds()

        order1 = PropertyOrder(
            property=PropertyReference("property1"),
            direction=Direction.ASCENDING,
        )
        order2 = PropertyOrder(
            property=PropertyReference("property2"),
            direction=Direction.DESCENDING,
        )
        q = Query(order=[order1, order2])
        assert q.to_ds()["order"] == [order1.to_ds(), order2.to_ds()]

    def test__to_ds__distinct_on(self):
        q = Query()
        assert "distinctOn" not in q.to_ds()

        prop1 = PropertyReference("property1")
        prop2 = PropertyReference("property2")
        q = Query(distinct_on=[prop1, prop2])
        assert q.to_ds()["distinctOn"] == [prop1.to_ds(), prop2.to_ds()]

    def test__to_ds__start_cursor(self):
        q = Query()
        assert "startCursor" not in q.to_ds()

        q = Query(start_cursor="cursor1")
        assert q.to_ds()["startCursor"] == "cursor1"

    def test__to_ds__end_cursor(self):
        q = Query()
        assert "endCursor" not in q.to_ds()

        q = Query(end_cursor="cursor1")
        assert q.to_ds()["endCursor"] == "cursor1"

    def test__to_ds__offset(self):
        q = Query()
        assert "offset" not in q.to_ds()

        q = Query(offset=123)
        assert q.to_ds()["offset"] == 123

    def test__to_ds__limit(self):
        q = Query()
        assert "limit" not in q.to_ds()

        q = Query(limit=100)
        assert q.to_ds()["limit"] == 100
