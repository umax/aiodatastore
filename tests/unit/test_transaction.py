import unittest

from aiodatastore.transaction import ReadOnlyOptions, ReadWriteOptions


class TestReadOnlyOptions(unittest.TestCase):
    def test__to_ds(self):
        opts = ReadOnlyOptions()
        assert opts.to_ds() == {
            "readOnly": {},
        }

    def test__init(self):
        ReadOnlyOptions()


class TestReadOWriteOptions(unittest.TestCase):
    def test__to_ds(self):
        opts = ReadWriteOptions("transaction1")
        assert opts.to_ds() == {
            "readWrite": {
                "previousTransaction": "transaction1",
            },
        }

    def test__init(self):
        opts = ReadWriteOptions("transaction1")
        assert opts.previous_transaction == "transaction1"
