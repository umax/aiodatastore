import unittest

from aiodatastore import (
    CommitResult,
    Key,
    MutationResult,
    PathElement,
    PartitionId,
)


class TestMutationResult(unittest.TestCase):
    def test__init__default_params(self):
        key = Key(PartitionId("project1"), [PathElement("kind1")])
        mr = MutationResult("version1", key)
        assert mr.version == "version1"
        assert mr.key == key
        assert mr.conflict_detected is False

    def test__init__custom_params(self):
        key = Key(PartitionId("project1"), [PathElement("kind1")])
        mr = MutationResult("version1", key, conflict_detected=True)
        assert mr.conflict_detected is True

    def test__from_ds(self):
        mr = MutationResult.from_ds(
            {
                "key": {
                    "partitionId": {
                        "projectId": "project1",
                    },
                    "path": [
                        {
                            "kind": "kind1",
                        },
                    ],
                },
                "version": "version1",
                "conflictDetected": True,
            }
        )
        assert isinstance(mr, MutationResult)
        assert mr.key == Key(PartitionId("project1"), [PathElement("kind1")])
        assert mr.version == "version1"
        assert mr.conflict_detected is True


class TestCommitResult(unittest.TestCase):
    def test__init(self):
        key = Key(PartitionId("project1"), [PathElement("kind1")])
        mr = MutationResult(key, "version1")
        cr = CommitResult(
            mutation_results=[mr],
            index_updates=123,
        )
        assert cr.mutation_results == [mr]
        assert cr.index_updates == 123

    def test__from_ds(self):
        mr_data = {
            "key": {
                "partitionId": {
                    "projectId": "project1",
                },
                "path": [
                    {
                        "kind": "kind1",
                    },
                ],
            },
            "version": "version1",
            "conflictDetected": True,
        }
        cr = CommitResult.from_ds(
            {
                "mutationResults": [mr_data],
                "indexUpdates": 123,
            }
        )
        assert isinstance(cr, CommitResult)
        assert cr.mutation_results == [MutationResult.from_ds(mr_data)]
        assert cr.index_updates == 123
