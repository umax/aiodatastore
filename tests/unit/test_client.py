import os
import unittest
from unittest import mock

from aiodatastore import Datastore, ReadConsistency


class TestDatastore(unittest.TestCase):
    def test_init__project_id__param(self):
        ds = Datastore(project_id="project1")
        assert ds._project_id == "project1"

    @mock.patch("aiodatastore.client.Datastore._get_project_id")
    def test_init__project_id__environ(self, mock_get_project_id):
        mock_get_project_id.return_value = "project1"
        ds = Datastore()
        assert ds._project_id == "project1"

    def test_init__namespace__param(self):
        ds = Datastore(project_id="project1", namespace="ns1")
        assert ds._namespace == "ns1"

    @mock.patch("aiodatastore.client.Datastore._get_namespace")
    def test_init__namespace__environ(self, mock_get_namespace):
        mock_get_namespace.return_value = "ns1"
        ds = Datastore(project_id="project1")
        assert ds._namespace == "ns1"

    def test__get_project_id(self):
        ds = Datastore(project_id="project1")

        with mock.patch.dict(os.environ, {"DATASTORE_PROJECT_ID": "proj1"}):
            assert ds._get_project_id() == "proj1"

        with mock.patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "proj2"}):
            assert ds._get_project_id() == "proj2"

    def test_no_project_id(self):
        with self.assertRaises(RuntimeError):
            Datastore()

    def test__get_namespace(self):
        ds = Datastore(project_id="project1")

        with mock.patch.dict(os.environ, {"DATASTORE_NAMESPACE": "ns1"}):
            assert ds._get_namespace() == "ns1"

        with mock.patch.dict(os.environ, {}):
            assert ds._get_namespace() == ""

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
