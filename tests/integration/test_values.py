import datetime
import uuid

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
    partition_id = PartitionId(PROJECT_ID)
    path_el = PathElement("TestEntity", name=str(uuid.uuid4()))
    key = Key(partition_id, [path_el])
    return Entity(key, properties=props)


class TestNullValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity = make_entity({"null-value": NullValue()})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)

            result = await ds.lookup([entity.key])
            assert len(result.found) == 1


class TestBooleanValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity1 = make_entity({"true-value": BooleanValue(True)})
        entity2 = make_entity({"false-value": BooleanValue(False)})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)

            result = await ds.lookup([entity1.key, entity2.key])
            assert len(result.found) == 2


class TestStringValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity1 = make_entity({"empty-string": StringValue("")})
        entity2 = make_entity({"some-string": StringValue("str1")})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)

            result = await ds.lookup([entity1.key, entity2.key])
            assert len(result.found) == 2


class TestIntegerValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity1 = make_entity({"negative-int": IntegerValue(-123)})
        entity2 = make_entity({"zero-int": IntegerValue(0)})
        entity3 = make_entity({"positive-int": IntegerValue(123)})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)
            await ds.insert(entity3)

            result = await ds.lookup([entity1.key, entity2.key, entity3.key])
            assert len(result.found) == 3


class TestDoubleValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity1 = make_entity({"negative-double": DoubleValue(-1.23)})
        entity2 = make_entity({"zero-double": DoubleValue(0.0)})
        entity3 = make_entity({"positive-double": DoubleValue(1.23)})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity1)
            await ds.insert(entity2)
            await ds.insert(entity3)

            result = await ds.lookup([entity1.key, entity2.key, entity3.key])
            assert len(result.found) == 3


class TestTimestampValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity = make_entity(
            {"datetime-value": TimestampValue(datetime.datetime.utcnow())}
        )
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)

            result = await ds.lookup([entity.key])
            assert len(result.found) == 1


class TestBlobValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity = make_entity({"blob-value": BlobValue(b"data")})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)

            result = await ds.lookup([entity.key])
            assert len(result.found) == 1


class TestArrayValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity = make_entity(
            {
                "array-value": ArrayValue(
                    [
                        NullValue(),
                        BooleanValue(True),
                        StringValue("str1"),
                        IntegerValue(123),
                        DoubleValue(1.23),
                        TimestampValue(datetime.datetime.utcnow()),
                        BlobValue(b"data"),
                    ]
                )
            }
        )
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)

            result = await ds.lookup([entity.key])
            assert len(result.found) == 1


class TestGeoPointValue:
    @pytest.mark.asyncio
    async def test__create(self):
        entity = make_entity({"geo-value": GeoPointValue(LatLng(1.23, 4.56))})
        async with Datastore(project_id=PROJECT_ID) as ds:
            await ds.insert(entity)

            result = await ds.lookup([entity.key])
            assert len(result.found) == 1
