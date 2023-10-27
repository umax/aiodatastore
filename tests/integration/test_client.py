import uuid

import pytest
from aiodatastore import Datastore, Entity, Key, PartitionId, PathElement

PROJECT_ID = "test"


class TestClient:
    @pytest.mark.asyncio
    async def test__lookup(self):
        partition_id = PartitionId(PROJECT_ID)
        key1 = Key(partition_id, [PathElement("TestKind", name=str(uuid.uuid4()))])
        key2 = Key(partition_id, [PathElement("TestKind", name=str(uuid.uuid4()))])

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
            [PathElement("TestKind", name=str(uuid.uuid4()))],
        )

        async with Datastore(project_id=PROJECT_ID) as ds:
            result = await ds.lookup([key])
            assert result.found == []
            assert len(result.missing) == 1
            assert result.missing[0].entity.key == key

            entity = Entity(key, properties={})
            await ds.insert(entity)

            result = await ds.lookup([key])
            assert result.missing == []
            assert len(result.found) == 1
            assert result.found[0].entity == entity
