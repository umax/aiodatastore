import unittest

from aiodatastore import PartitionId, PathElement, Key


class TestPartitionId(unittest.TestCase):
    def test__from_ds(self):
        p = PartitionId.from_ds(
            {
                "projectId": "project1",
            }
        )
        assert p.project_id == "project1"
        assert p.namespace_id is None

        p = PartitionId.from_ds(
            {
                "projectId": "project1",
                "namespaceId": "namespace1",
            }
        )
        assert p.project_id == "project1"
        assert p.namespace_id == "namespace1"

    def test__to_ds(self):
        p = PartitionId("project1")
        assert p.to_ds() == {
            "projectId": "project1",
        }

        p = PartitionId("project1", namespace_id="namespace1")
        assert p.to_ds() == {
            "projectId": "project1",
            "namespaceId": "namespace1",
        }

    def test_init(self):
        p = PartitionId("project1")
        assert p.project_id == "project1"
        assert p.namespace_id is None

        p = PartitionId("project1", namespace_id="namespace1")
        assert p.project_id == "project1"
        assert p.namespace_id == "namespace1"


class TestPathElement(unittest.TestCase):
    def test__from_ds(self):
        pe = PathElement.from_ds(
            {
                "kind": "kind1",
            }
        )
        assert pe.kind == "kind1"
        assert pe.id is None
        assert pe.name is None

        pe = PathElement.from_ds(
            {
                "kind": "kind1",
                "id": "id1",
            }
        )
        assert pe.kind == "kind1"
        assert pe.id == "id1"
        assert pe.name is None

        pe = PathElement.from_ds(
            {
                "kind": "kind1",
                "name": "name1",
            }
        )
        assert pe.kind == "kind1"
        assert pe.id is None
        assert pe.name == "name1"

    def test__to_ds(self):
        pe = PathElement("kind1", id="id1")
        assert pe.to_ds() == {
            "kind": "kind1",
            "id": "id1",
        }

        pe = PathElement("kind1", name="name1")
        assert pe.to_ds() == {
            "kind": "kind1",
            "name": "name1",
        }

    def test_init(self):
        pe = PathElement("kind1")
        assert pe.kind == "kind1"
        assert pe.id is None
        assert pe.name is None

        pe = PathElement("kind1", id="id1")
        assert pe.kind == "kind1"
        assert pe.id == "id1"
        assert pe.name is None

        pe = PathElement("kind1", name="name1")
        assert pe.kind == "kind1"
        assert pe.id is None
        assert pe.name == "name1"


class TestKey:
    def test__from_ds(self):
        key = Key.from_ds(
            {
                "partitionId": {
                    "projectId": "project1",
                },
                "path": [
                    {
                        "kind": "kind1",
                        "id": "id1",
                    },
                ],
            }
        )

        partition = key.partition_id
        assert isinstance(partition, PartitionId)
        assert partition.project_id == "project1"
        assert partition.namespace_id is None

        path = key.path
        assert isinstance(path, list)
        assert len(path) == 1
        assert path[0].kind == "kind1"
        assert path[0].id == "id1"

    def test__to_ds(self):
        partition = PartitionId("project1", namespace_id="namespace1")
        path = [
            PathElement("kind1", id="id1"),
            PathElement("kind2", name="name2"),
        ]
        key = Key(partition, path)
        assert key.to_ds() == {
            "partitionId": {
                "projectId": "project1",
                "namespaceId": "namespace1",
            },
            "path": [
                {
                    "kind": "kind1",
                    "id": "id1",
                },
                {
                    "kind": "kind2",
                    "name": "name2",
                },
            ],
        }

    def test_create(self):
        partition = PartitionId("project1", namespace_id="namespace1")
        path_element = PathElement("kind1", id="id1")
        key = Key(partition, [path_element])
        assert key.partition_id == partition
        assert key.path == [path_element]
