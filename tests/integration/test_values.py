import datetime

import pytest
from aiodatastore import (
    Datastore,
    Entity,
    Key,
    PartitionId,
    PathElement,
    NullValue,
    BooleanValue,
    StringValue,
    IntegerValue,
    DoubleValue,
    TimestampValue,
    BlobValue,
    ArrayValue,
    GeoPointValue,
    LatLng,
)

PROJECT_ID = "test"


def make_entity(props):
    key = Key(PartitionId(PROJECT_ID), [PathElement("TestKind")])
    return Entity(key, properties=props)


class TestNullValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity = make_entity({"null-field": NullValue()})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)


class TestBooleanValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity1 = make_entity({"bool-field": BooleanValue(True)})
        entity2 = make_entity({"bool-field": BooleanValue(False)})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)


class TestStringValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity1 = make_entity({"str-field": StringValue("")})
        entity2 = make_entity({"str-field": StringValue("str1")})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)


class TestIntegerValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity1 = make_entity({"int-field": IntegerValue(-123)})
        entity2 = make_entity({"int-field": IntegerValue(0)})
        entity3 = make_entity({"int-field": IntegerValue(123)})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)
            await ds.insert(entity3)


class TestDoubleValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity1 = make_entity({"double-field": DoubleValue(-1.23)})
        entity2 = make_entity({"double-field": DoubleValue(0.0)})
        entity3 = make_entity({"double-field": DoubleValue(1.23)})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)
            await ds.insert(entity3)


class TestTimestampValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity = make_entity(
            {"timestamp-field": TimestampValue(datetime.datetime.utcnow())}
        )
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)


class TestBlobValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity = make_entity({"blob-field": BlobValue("data")})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)


class TestArrayValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity = make_entity(
            {
                "array-field": ArrayValue(
                    [
                        NullValue(),
                        BooleanValue(True),
                        StringValue("str1"),
                        IntegerValue(123),
                        DoubleValue(1.23),
                        TimestampValue(datetime.datetime.utcnow()),
                        BlobValue("data"),
                    ]
                )
            }
        )
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)


class TestGeoPointValue:
    @pytest.mark.asyncio
    async def test_create_ok(self):
        entity = make_entity({"geo-field": GeoPointValue(LatLng(1.23, 4.56))})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)
