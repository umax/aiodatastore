import uuid

import pytest
from aiodatastore import Datastore, Entity, Key, PartitionId, PathElement, StringValue

PROJECT_ID = "test"


class TestClient:
    @pytest.mark.asyncio
    async def test__lookup(self):
        partition_id = PartitionId(PROJECT_ID)
        key1 = Key(partition_id, [PathElement("TestEntity", name=str(uuid.uuid4()))])
        key2 = Key(partition_id, [PathElement("TestEntity", name=str(uuid.uuid4()))])

        async with Datastore(project_id=PROJECT_ID) as ds:
            entity1 = Entity(key1, properties={})
            entity2 = Entity(key2, properties={})
            await ds.insert(entity1)

            result = await ds.lookup([key1, key2])
            assert result.deferred == []

            assert len(result.missing) == 1
            assert result.missing[0].entity == entity2

            assert len(result.found) == 1
            assert result.found[0].entity == entity1

    @pytest.mark.asyncio
    async def test__insert(self):
        key = Key(
            PartitionId(PROJECT_ID),
            [PathElement("TestEntity", name=str(uuid.uuid4()))],
        )

        async with Datastore(project_id=PROJECT_ID) as ds:
            # check key doesn't exist
            result = await ds.lookup([key])
            assert result.found == []
            assert len(result.missing) == 1
            assert result.missing[0].entity.key == key

            # insert an entity
            entity = Entity(key, properties={})
            await ds.insert(entity)

            # check created ok
            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity

    @pytest.mark.asyncio
    async def test__upsert__entity_not_exist(self):
        key = Key(
            PartitionId(PROJECT_ID),
            [PathElement("TestEntity", name=str(uuid.uuid4()))],
        )

        async with Datastore(project_id=PROJECT_ID) as ds:
            # check key doesn't exist
            result = await ds.lookup([key])
            assert result.found == []
            assert len(result.missing) == 1
            assert result.missing[0].entity.key == key

            # upsert an entity
            entity = Entity(key, properties={})
            await ds.upsert(entity)

            # check created ok
            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity

    @pytest.mark.asyncio
    async def test__upsert__entity_exist(self):
        key = Key(
            PartitionId(PROJECT_ID),
            [PathElement("TestEntity", name=str(uuid.uuid4()))],
        )

        async with Datastore(project_id=PROJECT_ID) as ds:
            # check key doesn't exist
            result = await ds.lookup([key])
            assert result.found == []
            assert len(result.missing) == 1
            assert result.missing[0].entity.key == key

            # create new entity
            entity = Entity(key, properties={})
            await ds.insert(entity)

            # check created ok
            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity

            # update entity property
            entity.properties["str-value"] = StringValue("str1")
            await ds.upsert(entity)

            # check updated ok
            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity
            assert result.found[0].entity.properties["str-value"].value == "str1"

    @pytest.mark.asyncio
    async def test__update(self):
        key = Key(
            PartitionId(PROJECT_ID),
            [PathElement("TestEntity", name=str(uuid.uuid4()))],
        )

        async with Datastore(project_id=PROJECT_ID) as ds:
            # create an entity
            entity = Entity(key, properties={"str-value": StringValue("str1")})
            result = await ds.insert(entity)

            # check created ok
            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity

            # update entity property
            entity.properties["str-value"].value = "str2"
            await ds.update(entity)

            # check updated ok
            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity
            assert result.found[0].entity.properties["str-value"].value == "str2"

    @pytest.mark.asyncio
    async def test__delete(self):
        key = Key(
            PartitionId(PROJECT_ID),
            [PathElement("TestEntity", name=str(uuid.uuid4()))],
        )

        async with Datastore(project_id=PROJECT_ID) as ds:
            # create an entity
            entity = Entity(key, properties={})
            result = await ds.insert(entity)

            # check created ok
            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity

            # delete an entity
            await ds.delete(key)

            # check deleted ok
            result = await ds.lookup([key])
            assert result.found == []
            assert len(result.missing) == 1
            assert result.missing[0].entity.key == key
