import unittest

from aiodatastore.constants import (
    Direction,
    MoreResultsType,
    PropertyFilterOperator,
    ResultType,
)
from aiodatastore.entity import Entity, EntityResult
from aiodatastore.filters import PropertyFilter
from aiodatastore.key import Key, PartitionId, PathElement
from aiodatastore.property import PropertyOrder, PropertyReference
from aiodatastore.query import KindExpression, Projection, Query, QueryResultBatch
from aiodatastore.values import IntegerValue


class TestProjection(unittest.TestCase):
    def test__init(self):
        p = Projection(PropertyReference("property1"))
        assert p.property == PropertyReference("property1")

    def test__eq(self):
        proj1 = Projection(PropertyReference("prop1"))
        proj2 = Projection(PropertyReference("prop2"))
        proj3 = Projection(PropertyReference("prop1"))
        assert proj1 == proj3
        assert proj1 != proj2

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


class TestKindExpression(unittest.TestCase):
    def test__init(self):
        ke = KindExpression("kind1")
        assert ke.name == "kind1"

    def test__eq(self):
        assert KindExpression("name1") == KindExpression("name1")
        assert KindExpression("name1") != KindExpression("name2")

    def test__from_ds(self):
        ke = KindExpression.from_ds({"name": "kind1"})
        assert isinstance(ke, KindExpression)
        assert ke.name == "kind1"

    def test__to_ds(self):
        ke = KindExpression("kind1")
        assert ke.to_ds() == {"name": "kind1"}


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


class TestQueryResultBatch(unittest.TestCase):
    def setUp(self):
        key = Key(PartitionId("project1"), [PathElement("kind1")])
        self.er = EntityResult(Entity(key, {}))

    def test__init__default_params(self):
        qrb = QueryResultBatch()
        assert qrb.skipped_results == 0
        assert qrb.skipped_cursor is None
        assert qrb.entity_result_type == ResultType.UNSPECIFIED
        assert qrb.entity_results == []
        assert qrb.end_cursor == ""
        assert qrb.more_results == MoreResultsType.UNSPECIFIED
        assert qrb.snapshot_version == ""

    def test__init__custom_params(self):
        qrb = QueryResultBatch(
            skipped_results=1,
            skipped_cursor="skipped-cursor",
            entity_result_type=ResultType.KEY_ONLY,
            entity_results=[self.er],
            end_cursor="end-cursor",
            more_results=MoreResultsType.NO_MORE_RESULTS,
            snapshot_version="snapshot-version",
        )
        assert qrb.skipped_results == 1
        assert qrb.skipped_cursor == "skipped-cursor"
        assert qrb.entity_result_type == ResultType.KEY_ONLY
        assert qrb.entity_results == [self.er]
        assert qrb.end_cursor == "end-cursor"
        assert qrb.more_results == MoreResultsType.NO_MORE_RESULTS
        assert qrb.snapshot_version == "snapshot-version"

    def test__from_ds(self):
        qrb = QueryResultBatch.from_ds(
            {
                "skippedResults": 123,
                "skippedCursor": "skipped-cursor",
                "entityResultType": "FULL",
                "entityResults": [self.er.to_ds()],
                "endCursor": "end-cursor",
                "moreResults": "NO_MORE_RESULTS",
                "snapshotVersion": "snapshot-version",
            }
        )
        assert isinstance(qrb, QueryResultBatch)
        assert qrb.skipped_results == 123
        assert qrb.skipped_cursor == "skipped-cursor"
        assert qrb.entity_result_type == ResultType.FULL
        assert qrb.entity_results == [self.er]
        assert qrb.end_cursor == "end-cursor"
        assert qrb.more_results == MoreResultsType.NO_MORE_RESULTS
        assert qrb.snapshot_version == "snapshot-version"

    def test__to_ds__mandatory_params(self):
        qrb = QueryResultBatch([self.er])
        qrb.to_ds() == {
            "endCursor": qrb.end_cursor,
            "entityResults": [self.er.to_ds()],
            "entityResultType": qrb.entity_result_type.value,
            "moreResults": qrb.more_results.value,
            "snapshotVersion": qrb.snapshot_version,
            "skippedResults": qrb.skipped_results,
        }

    def test__to_ds__all_params(self):
        qrb = QueryResultBatch([self.er], skipped_cursor="skipped-cursor")
        qrb.to_ds() == {
            "endCursor": qrb.end_cursor,
            "entityResults": [self.er.to_ds()],
            "entityResultType": qrb.entity_result_type.value,
            "moreResults": qrb.more_results.value,
            "snapshotVersion": qrb.snapshot_version,
            "skippedResults": qrb.skipped_results,
            "skippedCursor": "skipped-cursor",
        }
