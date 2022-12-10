import os
import unittest
from unittest import mock

from aiodatastore import Datastore, ReadConsistency


class TestDatastore(unittest.TestCase):
    def test__init__project_id__param(self):
        ds = Datastore(project_id="project1")
        assert ds._project_id == "project1"

    def test__init__namespace__param(self):
        ds = Datastore(project_id="project1", namespace="ns1")
        assert ds._namespace == "ns1"

    @mock.patch.dict(os.environ, {"DATASTORE_EMULATOR_HOST": "host1"})
    def test__init__emulator_mode(self):
        ds = Datastore(project_id="project1")
        assert ds._token is None

    def test__get_read_options__transaction_id(self):
        ds = Datastore(project_id="project1")
        opts = ds._get_read_options(
            consistency=ReadConsistency.EVENTUAL,
            transaction_id="transaction1",
        )
        assert opts == {"transaction": "transaction1"}

    def test__get_read_options__no_transaction_id(self):
        ds = Datastore(project_id="project1")
        opts = ds._get_read_options(
            consistency=ReadConsistency.EVENTUAL,
            transaction_id=None,
        )
        assert opts == {"readConsistency": "EVENTUAL"}

    def test__get_partition_id(self):
        ds = Datastore(project_id="project1", namespace="namespace1")
        assert ds._get_partition_id() == {
            "projectId": "project1",
            "namespaceId": "namespace1",
        }
