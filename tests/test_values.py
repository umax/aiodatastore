import unittest
from datetime import datetime

from aiodatastore import (
    BooleanValue,
    NullValue,
    StringValue,
    IntegerValue,
    DoubleValue,
    TimestampValue,
    ArrayValue,
    GeoPointValue,
    LatLng,
    BlobValue,
    KeyValue,
    Key,
)


class TestNullValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = NullValue(raw_value="NULL_VALUE")
        assert value.raw_to_py() is None

    def test__py_to_raw(self):
        value = NullValue(value=None)
        assert value.py_to_raw() == "NULL_VALUE"

    def test__to_ds(self):
        value = NullValue(value=None, indexed=False)
        value.to_ds() == {
            "nullValue": "NULL_VALUE",
            "excludeFromIndexes": True,
        }

        value = NullValue(value=None, indexed=True)
        value.to_ds() == {
            "nullValue": "NULL_VALUE",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert NullValue() == NullValue()

    def test_create_from_py(self):
        value = NullValue(value=None, indexed=True)
        assert value.py_value is None
        assert value.raw_value is None
        assert value.indexed is True

        value = NullValue(indexed=False)
        assert value.py_value is None
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = NullValue(raw_value="NULL_VALUE")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestBooleanValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = BooleanValue(raw_value=False)
        assert value.raw_to_py() is False

        value = BooleanValue(raw_value=True)
        assert value.raw_to_py() is True

    def test__py_to_raw(self):
        value = BooleanValue(value=True)
        assert value.py_to_raw() is True

        value = BooleanValue(value=False)
        assert value.py_to_raw() is False

    def test__to_ds(self):
        value = BooleanValue(value=True, indexed=False)
        value.to_ds() == {
            "booleanValue": True,
            "excludeFromIndexes": True,
        }

        value = BooleanValue(value=False, indexed=True)
        value.to_ds() == {
            "booleanValue": False,
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert BooleanValue(value=False) == BooleanValue(value=False)
        assert BooleanValue(value=True) != BooleanValue(value=False)

    def test_setter(self):
        value = BooleanValue()
        assert value.py_value is None
        value.value = True
        assert value.py_value is True
        value.value = False
        assert value.py_value is False

    def test_create_from_py(self):
        value = BooleanValue(value=False, indexed=True)
        assert value.py_value is False
        assert value.raw_value is None
        assert value.indexed is True

        value = BooleanValue(value=True, indexed=False)
        assert value.py_value is True
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = BooleanValue(raw_value=True)
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestStringValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = StringValue(raw_value="string1")
        assert value.raw_to_py() == "string1"

        value = StringValue(raw_value="")
        assert value.raw_to_py() == ""

    def test__py_to_raw(self):
        value = StringValue(value="string1")
        assert value.py_to_raw() == "string1"

        value = StringValue(value="")
        assert value.py_to_raw() == ""

    def test__to_ds(self):
        value = StringValue(value="string1", indexed=False)
        value.to_ds() == {
            "stringValue": "string1",
            "excludeFromIndexes": True,
        }

        value = StringValue(value="", indexed=True)
        value.to_ds() == {
            "stringValue": "",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert StringValue(value="str1") == StringValue(value="str1")
        assert StringValue(value="str1") != StringValue(value="str2")

    def test_setter(self):
        value = StringValue()
        assert value.py_value is None
        value.value = "str1"
        assert value.py_value == "str1"
        value.value = "str2"
        assert value.py_value == "str2"

    def test_create_from_py(self):
        value = StringValue(value="string1", indexed=True)
        assert value.py_value == "string1"
        assert value.raw_value is None
        assert value.indexed is True

        value = StringValue(value="", indexed=False)
        assert value.py_value == ""
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = StringValue(raw_value="string1")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestIntegerValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = IntegerValue(raw_value="123")
        assert value.raw_to_py() == 123

        value = IntegerValue(raw_value="0")
        assert value.raw_to_py() == 0

        value = IntegerValue(raw_value="-123")
        assert value.raw_to_py() == -123

    def test__py_to_raw(self):
        value = IntegerValue(value=123)
        assert value.py_to_raw() == "123"

        value = IntegerValue(value=0)
        assert value.py_to_raw() == "0"

        value = IntegerValue(value=-123)
        assert value.py_to_raw() == "-123"

    def test__to_ds(self):
        value = IntegerValue(value=123, indexed=False)
        value.to_ds() == {
            "integerValue": "123",
            "excludeFromIndexes": True,
        }

        value = IntegerValue(value=0, indexed=True)
        value.to_ds() == {
            "integerValue": "0",
            "excludeFromIndexes": False,
        }

        value = IntegerValue(value=-123, indexed=True)
        value.to_ds() == {
            "integerValue": "-123",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert IntegerValue(value=123) == IntegerValue(value=123)
        assert IntegerValue(value=123) != IntegerValue(value=122)

    def test_setter(self):
        value = IntegerValue()
        assert value.py_value is None
        value.value = 1
        assert value.py_value == 1
        value.value = 2
        assert value.py_value == 2

    def test_create_from_py(self):
        value = IntegerValue(value=123, indexed=True)
        assert value.py_value == 123
        assert value.raw_value is None
        assert value.indexed is True

        value = IntegerValue(value=0, indexed=False)
        assert value.py_value == 0
        assert value.raw_value is None
        assert value.indexed is False

        value = IntegerValue(value=-123, indexed=False)
        assert value.py_value == -123
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = IntegerValue(raw_value="123")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestDoubleValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = DoubleValue(raw_value=1.23)
        assert value.raw_to_py() == 1.23

        value = DoubleValue(raw_value=0.0)
        assert value.raw_to_py() == 0.0

        value = DoubleValue(raw_value=-1.23)
        assert value.raw_to_py() == -1.23

    def test__py_to_raw(self):
        value = DoubleValue(value=1.23)
        assert value.py_to_raw() == 1.23

        value = DoubleValue(value=0.0)
        assert value.py_to_raw() == 0.0

        value = DoubleValue(value=-1.23)
        assert value.py_to_raw() == -1.23

    def test__to_ds(self):
        value = DoubleValue(value=1.23, indexed=False)
        value.to_ds() == {
            "doubleValue": 1.23,
            "excludeFromIndexes": True,
        }

        value = DoubleValue(value=0.0, indexed=True)
        value.to_ds() == {
            "doubleValue": 0.0,
            "excludeFromIndexes": False,
        }

        value = DoubleValue(value=-1.23, indexed=True)
        value.to_ds() == {
            "doubleValue": -1.23,
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert DoubleValue(value=1.23) == DoubleValue(value=1.23)
        assert DoubleValue(value=1.23) != DoubleValue(value=1.24)

    def test_setter(self):
        value = DoubleValue()
        assert value.py_value is None
        value.value = 1.23
        assert value.py_value == 1.23
        value.value = 4.56
        assert value.py_value == 4.56

    def test_create_from_py(self):
        value = DoubleValue(value=1.23, indexed=True)
        assert value.py_value == 1.23
        assert value.raw_value is None
        assert value.indexed is True

        value = DoubleValue(value=0.0, indexed=False)
        assert value.py_value == 0.0
        assert value.raw_value is None
        assert value.indexed is False

        value = DoubleValue(value=-1.23, indexed=False)
        assert value.py_value == -1.23
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = DoubleValue(raw_value=1.23)
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestTimestampValue(unittest.TestCase):
    dt = datetime(
        year=2022,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
        microsecond=123456,
    )

    def test__raw_to_py(self):
        value = TimestampValue(raw_value="2022-01-01T01:02:03.123456000Z")
        assert value.raw_to_py() == datetime(
            year=2022,
            month=1,
            day=1,
            hour=1,
            minute=2,
            second=3,
            microsecond=123456,
        )

        value = TimestampValue(raw_value="2022-01-02T03:04:05.123456Z")
        assert value.raw_to_py() == datetime(
            year=2022,
            month=1,
            day=2,
            hour=3,
            minute=4,
            second=5,
            microsecond=123456,
        )

    def test__py_to_raw(self):
        value = TimestampValue(value=self.dt)
        assert value.py_to_raw() == "2022-01-02T03:04:05.123456"

    def test__to_ds(self):
        value = TimestampValue(value=self.dt, indexed=False)
        value.to_ds() == {
            "timestampValue": "2022-01-02T03:04:05.123456",
            "excludeFromIndexes": True,
        }

        value = TimestampValue(value=self.dt, indexed=True)
        value.to_ds() == {
            "doubleValue": "2022-01-02T03:04:05.123456",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        t1 = TimestampValue(value="2022-01-02T03:04:05.123456")
        t2 = TimestampValue(value="2022-01-02T03:04:05.123456")
        assert t1 == t2

        t1 = TimestampValue(value="2022-01-02T03:04:05.123456")
        t2 = TimestampValue(value="2022-01-02T03:04:05.123457")
        assert t1 != t2

    def test_setter(self):
        value = TimestampValue()
        assert value.py_value is None
        value.value = self.dt
        assert value.py_value == self.dt

    def test_create_from_py(self):
        value = TimestampValue(value=self.dt, indexed=True)
        assert value.py_value == self.dt
        assert value.raw_value is None
        assert value.indexed is True

    def test_value_property(self):
        value = TimestampValue(raw_value="2022-01-02T03:04:05.123456Z")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestArrayValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = ArrayValue(raw_value={"values": []})
        assert value.raw_to_py() == []

        value = ArrayValue(
            raw_value={
                "values": [
                    {
                        "nullValue": "NULL_VALUE",
                        "excludeFromIndexes": True,
                    },
                    {
                        "stringValue": "string1",
                        "excludeFromIndexes": True,
                    },
                    {
                        "booleanValue": False,
                        "excludeFromIndexes": False,
                    },
                ],
            }
        )
        value = value.raw_to_py()
        assert len(value) == 3

        el1 = value[0]
        assert el1.raw_value == "NULL_VALUE"
        assert el1.py_value is None
        assert el1.indexed is False

        el2 = value[1]
        assert el2.raw_value == "string1"
        assert el2.py_value is None
        assert el2.indexed is False

        el3 = value[2]
        assert el3.raw_value is False
        assert el3.py_value is None
        assert el3.indexed is True

    def test__py_to_raw(self):
        el1 = NullValue(indexed=True)
        el2 = StringValue(value="string1", indexed=False)
        el3 = IntegerValue(value=123, indexed=True)
        value = ArrayValue(value=[el1, el2, el3])
        assert value.py_to_raw() == [
            el1.to_ds(),
            el2.to_ds(),
            el3.to_ds(),
        ]

    def test__to_ds(self):
        value = ArrayValue(value=[], indexed=False)
        value.to_ds() == {
            "arrayValue": {
                "values": [],
            },
        }

        el1 = NullValue(indexed=True)
        el2 = StringValue(value="string1", indexed=False)
        el3 = IntegerValue(value=123, indexed=True)
        value = ArrayValue(value=[el1, el2, el3], indexed=True)
        value.to_ds() == {
            "arrayValue": {
                "values": [
                    el1.to_ds(),
                    el2.to_ds(),
                    el3.to_ds(),
                ],
            },
        }

    def test_eq(self):
        a1 = ArrayValue(value=[BooleanValue(True), NullValue()])
        a2 = ArrayValue(value=[BooleanValue(True), NullValue()])
        assert a1 == a2

        a1 = ArrayValue(value=[BooleanValue(True), NullValue()])
        a2 = ArrayValue(value=[NullValue(), BooleanValue(True)])
        assert a1 != a2

    def test_setter(self):
        value = ArrayValue()
        assert value.py_value is None
        value.value = [NullValue(), BooleanValue(True)]
        assert value.py_value == [NullValue(), BooleanValue(True)]
        value.value = [BooleanValue(False)]
        assert value.py_value == [BooleanValue(False)]

    def test_create_from_py(self):
        value = ArrayValue(value=[], indexed=True)
        assert value.py_value == []
        assert value.raw_value is None
        assert value.indexed is True

    def test_value_property(self):
        value = ArrayValue(raw_value={"values": []})
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestGeoPointValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = GeoPointValue(
            raw_value={
                "latitude": 1.23,
                "longitude": 4.56,
            }
        )
        value = value.raw_to_py()
        assert isinstance(value, LatLng)
        assert value.lat == 1.23
        assert value.lng == 4.56

    def test__py_to_raw(self):
        value = GeoPointValue(value=LatLng(lat=1.23, lng=4.56))
        assert value.py_to_raw() == {
            "latitude": 1.23,
            "longitude": 4.56,
        }

    def test__to_ds(self):
        value = GeoPointValue(value=LatLng(lat=1.23, lng=4.56), indexed=True)
        value.to_ds() == {
            "geoPointValue": {
                "latitude": 1.23,
                "longitude": 4.56,
            },
            "excludeFromIndexes": False,
        }

        value = GeoPointValue(value=LatLng(lat=1.23, lng=4.56), indexed=False)
        value.to_ds() == {
            "geoPointValue": {
                "latitude": 1.23,
                "longitude": 4.56,
            },
            "excludeFromIndexes": True,
        }

    def test_eq(self):
        gp1 = GeoPointValue(value=LatLng(lat=1.23, lng=4.56))
        gp2 = GeoPointValue(value=LatLng(lat=1.23, lng=4.56))
        assert gp1 == gp2

        gp1 = GeoPointValue(value=LatLng(lat=1.23, lng=4.56))
        gp2 = GeoPointValue(value=LatLng(lat=1.23, lng=4.57))
        assert gp1 != gp2

    def test_create_from_py(self):
        lat_lng = LatLng(lat=1.23, lng=4.56)
        value = GeoPointValue(value=lat_lng, indexed=False)
        assert value.py_value == lat_lng
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = GeoPointValue(raw_value={"latitude": 1.23, "longitude": 4.56})
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestBlobValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = BlobValue(raw_value="aGVsbG8h")
        assert value.raw_to_py() == b"hello!"

    def test__py_to_raw(self):
        value = BlobValue(value="hello!")
        assert value.py_to_raw() == "aGVsbG8h"

    def test__to_ds(self):
        value = BlobValue(value="hello!", indexed=True)
        value.to_ds() == {
            "blobValue": "aGVsbG8h",
            "excludeFromIndexes": False,
        }

        value = BlobValue(value="hello!", indexed=False)
        value.to_ds() == {
            "blobValue": "aGVsbG8h",
            "excludeFromIndexes": True,
        }

    def test_create_from_py(self):
        value = BlobValue(value="hello!", indexed=False)
        assert value.py_value == "hello!"
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = BlobValue(raw_value="aGVsbG8h")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()


class TestKeyValue(unittest.TestCase):
    raw_value = {
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

    def test__raw_to_py(self):
        value = KeyValue(raw_value=self.raw_value)
        assert value.raw_to_py() == Key.from_ds(self.raw_value)

    def test__py_to_raw(self):
        key = Key.from_ds(self.raw_value)
        value = KeyValue(value=key)
        assert value.py_to_raw() == self.raw_value

    def test__to_ds(self):
        key = Key.from_ds(self.raw_value)

        value = KeyValue(value=key, indexed=True)
        value.to_ds() == {
            "keyValue": key.to_ds(),
            "excludeFromIndexes": False,
        }

        value = KeyValue(value=key, indexed=False)
        value.to_ds() == {
            "keyValue": key.to_ds(),
            "excludeFromIndexes": True,
        }

    def test_create_from_py(self):
        key = Key.from_ds(self.raw_value)
        value = KeyValue(value=key, indexed=False)
        assert value.py_value == key
        assert value.raw_value is None
        assert value.indexed is False

    def test_value_property(self):
        value = KeyValue(raw_value=self.raw_value)
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()
