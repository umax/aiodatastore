import base64
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
    def test__to_ds(self):
        value = NullValue(indexed=False)
        assert value.to_ds() == {
            "nullValue": "NULL_VALUE",
            "excludeFromIndexes": True,
        }

        value = NullValue(indexed=True)
        assert value.to_ds() == {
            "nullValue": "NULL_VALUE",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert NullValue() == NullValue()

    def test_getter(self):
        value = NullValue()
        assert value.value is None

    def test_setter(self):
        value = NullValue()
        value.value = None
        assert value.value is None

    def test_create_from_py(self):
        value = NullValue(indexed=True)
        assert value.indexed is True

        value = NullValue(indexed=False)
        assert value.indexed is False


class TestBooleanValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = BooleanValue(None, raw_value=False)
        assert value.raw_to_py() is False

        value = BooleanValue(None, raw_value=True)
        assert value.raw_to_py() is True

    def test__py_to_raw(self):
        value = BooleanValue(True)
        assert value.py_to_raw() is True

        value = BooleanValue(False)
        assert value.py_to_raw() is False

    def test__to_ds__raw_value(self):
        value = BooleanValue(None, raw_value=False)
        assert value.to_ds() == {
            "booleanValue": False,
            "excludeFromIndexes": False,
        }

    def test__to_ds__py_value(self):
        value = BooleanValue(True, indexed=False)
        assert value.to_ds() == {
            "booleanValue": True,
            "excludeFromIndexes": True,
        }

        value = BooleanValue(False, indexed=True)
        assert value.to_ds() == {
            "booleanValue": False,
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert BooleanValue(False) == BooleanValue(False)
        assert BooleanValue(True) == BooleanValue(True)
        assert BooleanValue(True) != BooleanValue(False)

    def test_setter(self):
        value = BooleanValue(None)
        assert value.py_value is None
        value.value = True
        assert value.py_value is True
        value.value = False
        assert value.py_value is False

    def test_getter(self):
        value = BooleanValue(None, raw_value=True)
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        value = BooleanValue(False, indexed=True)
        assert value.py_value is False
        assert value.raw_value is None
        assert value.indexed is True

        value = BooleanValue(True, indexed=False)
        assert value.py_value is True
        assert value.raw_value is None
        assert value.indexed is False


class TestStringValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = StringValue(None, raw_value="string1")
        assert value.raw_to_py() == "string1"

        value = StringValue(None, raw_value="")
        assert value.raw_to_py() == ""

    def test__py_to_raw(self):
        value = StringValue("string1")
        assert value.py_to_raw() == "string1"

        value = StringValue("")
        assert value.py_to_raw() == ""

    def test__to_ds__raw_value(self):
        value = StringValue(None, raw_value="raw-value1")
        assert value.to_ds() == {
            "stringValue": "raw-value1",
            "excludeFromIndexes": False,
        }

    def test__to_ds__py_value(self):
        value = StringValue("string1", indexed=False)
        assert value.to_ds() == {
            "stringValue": "string1",
            "excludeFromIndexes": True,
        }

        value = StringValue("", indexed=True)
        assert value.to_ds() == {
            "stringValue": "",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert StringValue("str1") == StringValue("str1")
        assert StringValue("str1") != StringValue("str2")

    def test_setter(self):
        value = StringValue(None)
        assert value.py_value is None
        value.value = "str1"
        assert value.py_value == "str1"
        value.value = "str2"
        assert value.py_value == "str2"

    def test_getter(self):
        value = StringValue(None, raw_value="string1")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        value = StringValue("string1", indexed=True)
        assert value.py_value == "string1"
        assert value.raw_value is None
        assert value.indexed is True

        value = StringValue("", indexed=False)
        assert value.py_value == ""
        assert value.raw_value is None
        assert value.indexed is False


class TestIntegerValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = IntegerValue(None, raw_value="123")
        assert value.raw_to_py() == 123

        value = IntegerValue(None, raw_value="0")
        assert value.raw_to_py() == 0

        value = IntegerValue(None, raw_value="-123")
        assert value.raw_to_py() == -123

    def test__py_to_raw(self):
        value = IntegerValue(123)
        assert value.py_to_raw() == str(123)

        value = IntegerValue(0)
        assert value.py_to_raw() == str(0)

        value = IntegerValue(-123)
        assert value.py_to_raw() == str(-123)

    def test__to_ds__raw_value(self):
        value = IntegerValue(None, "123")
        assert value.to_ds() == {
            "integerValue": "123",
            "excludeFromIndexes": False,
        }

    def test__to_ds__py_value(self):
        value = IntegerValue(123, indexed=False)
        assert value.to_ds() == {
            "integerValue": "123",
            "excludeFromIndexes": True,
        }

        value = IntegerValue(0, indexed=True)
        assert value.to_ds() == {
            "integerValue": "0",
            "excludeFromIndexes": False,
        }

        value = IntegerValue(-123, indexed=True)
        assert value.to_ds() == {
            "integerValue": "-123",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert IntegerValue(123) == IntegerValue(123)
        assert IntegerValue(123) != IntegerValue(122)

    def test_setter(self):
        value = IntegerValue(None)
        assert value.py_value is None
        value.value = 1
        assert value.py_value == 1
        value.value = 2
        assert value.py_value == 2

    def test_getter(self):
        value = IntegerValue(None, raw_value="123")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        value = IntegerValue(123, indexed=True)
        assert value.py_value == 123
        assert value.raw_value is None
        assert value.indexed is True

        value = IntegerValue(0, indexed=False)
        assert value.py_value == 0
        assert value.raw_value is None
        assert value.indexed is False

        value = IntegerValue(-123, indexed=False)
        assert value.py_value == -123
        assert value.raw_value is None
        assert value.indexed is False


class TestDoubleValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = DoubleValue(None, raw_value=1.23)
        assert value.raw_to_py() == 1.23

        value = DoubleValue(None, raw_value=0.0)
        assert value.raw_to_py() == 0.0

        value = DoubleValue(None, raw_value=-1.23)
        assert value.raw_to_py() == -1.23

    def test__py_to_raw(self):
        value = DoubleValue(1.23)
        assert value.py_to_raw() == 1.23

        value = DoubleValue(0.0)
        assert value.py_to_raw() == 0.0

        value = DoubleValue(-1.23)
        assert value.py_to_raw() == -1.23

    def test__to_ds__raw_value(self):
        value = DoubleValue(None, raw_value=1.23)
        assert value.to_ds() == {
            "doubleValue": 1.23,
            "excludeFromIndexes": False,
        }

    def test__to_ds__py_value(self):
        value = DoubleValue(1.23, indexed=False)
        assert value.to_ds() == {
            "doubleValue": 1.23,
            "excludeFromIndexes": True,
        }

        value = DoubleValue(0.0, indexed=True)
        assert value.to_ds() == {
            "doubleValue": 0.0,
            "excludeFromIndexes": False,
        }

        value = DoubleValue(-1.23, indexed=True)
        assert value.to_ds() == {
            "doubleValue": -1.23,
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert DoubleValue(1.23) == DoubleValue(1.23)
        assert DoubleValue(1.23) != DoubleValue(1.24)

    def test_setter(self):
        value = DoubleValue(None)
        assert value.py_value is None
        value.value = 1.23
        assert value.py_value == 1.23
        value.value = 4.56
        assert value.py_value == 4.56

    def test_getter(self):
        value = DoubleValue(None, raw_value=1.23)
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        value = DoubleValue(1.23, indexed=True)
        assert value.py_value == 1.23
        assert value.raw_value is None
        assert value.indexed is True

        value = DoubleValue(0.0, indexed=False)
        assert value.py_value == 0.0
        assert value.raw_value is None
        assert value.indexed is False

        value = DoubleValue(-1.23, indexed=False)
        assert value.py_value == -1.23
        assert value.raw_value is None
        assert value.indexed is False


class TestTimestampValue(unittest.TestCase):
    dt1 = datetime(
        year=2022,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
        microsecond=123456,
    )
    dt2 = datetime(
        year=2023,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
        microsecond=123456,
    )

    def test__raw_to_py(self):
        value = TimestampValue(None, raw_value="2022-01-01T01:02:03.123456000Z")
        assert value.raw_to_py() == datetime(
            year=2022,
            month=1,
            day=1,
            hour=1,
            minute=2,
            second=3,
            microsecond=123456,
        )

        value = TimestampValue(None, raw_value="2022-01-02T03:04:05.123456Z")
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
        value = TimestampValue(self.dt1)
        assert value.py_to_raw() == "2022-01-02T03:04:05.123456Z"

    def test__to_ds__raw_value(self):
        value = TimestampValue(None, raw_value="raw-value1")
        assert value.to_ds() == {
            "timestampValue": "raw-value1",
            "excludeFromIndexes": False,
        }

    def test__to_ds__py_value(self):
        value = TimestampValue(self.dt1, indexed=False)
        assert value.to_ds() == {
            "timestampValue": "2022-01-02T03:04:05.123456Z",
            "excludeFromIndexes": True,
        }

        value = TimestampValue(self.dt1, indexed=True)
        assert value.to_ds() == {
            "timestampValue": "2022-01-02T03:04:05.123456Z",
            "excludeFromIndexes": False,
        }

    def test_eq(self):
        assert TimestampValue(self.dt1) == TimestampValue(self.dt1)
        assert TimestampValue(self.dt1) != TimestampValue(self.dt2)

    def test_setter(self):
        value = TimestampValue(None)
        assert value.py_value is None
        value.value = self.dt1
        assert value.py_value == self.dt1

    def test_getter(self):
        value = TimestampValue(None, raw_value="2022-01-02T03:04:05.123456Z")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        value = TimestampValue(self.dt1, indexed=True)
        assert value.py_value == self.dt1
        assert value.raw_value is None
        assert value.indexed is True


class TestArrayValue(unittest.TestCase):
    def test__raw_to_py__empty_array(self):
        value = ArrayValue(None, raw_value={"values": []})
        assert value.raw_to_py() == []

    def test__raw_to_py__unsupported_type(self):
        value = ArrayValue(None, raw_value={"values": [{"key": "value"}]})
        with self.assertRaises(RuntimeError):
            value.raw_to_py()

    def test__raw_to_py(self):
        value = ArrayValue(
            None,
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
            },
        )
        value = value.raw_to_py()
        assert len(value) == 3

        el1 = value[0]
        assert el1 == NullValue()
        assert el1.type_name == "nullValue"

        el2 = value[1]
        assert el2.type_name == "stringValue"
        assert el2.raw_value == "string1"
        assert el2.py_value is None
        assert el2.indexed is False

        el3 = value[2]
        assert el3.type_name == "booleanValue"
        assert el3.raw_value is False
        assert el3.py_value is None
        assert el3.indexed is True

    def test__py_to_raw(self):
        el1 = NullValue(indexed=True)
        el2 = StringValue("string1", indexed=False)
        el3 = IntegerValue(123, indexed=True)
        value = ArrayValue([el1, el2, el3])
        assert value.py_to_raw() == [
            el1.to_ds(),
            el2.to_ds(),
            el3.to_ds(),
        ]

    def test__to_ds__raw_value(self):
        value = ArrayValue(
            None,
            raw_value={
                "values": [
                    {
                        "booleanValue": True,
                        "excludeFromIndexes": False,
                    },
                ],
            },
        )
        assert value.to_ds() == {
            "arrayValue": {
                "values": [
                    {
                        "booleanValue": True,
                        "excludeFromIndexes": False,
                    },
                ],
            },
        }

    def test__to_ds__py_value(self):
        value = ArrayValue([], indexed=False)
        assert value.to_ds() == {
            "arrayValue": {
                "values": [],
            },
        }

        el1 = NullValue(indexed=True)
        el2 = StringValue("string1", indexed=False)
        el3 = IntegerValue(123, indexed=True)
        value = ArrayValue([el1, el2, el3], indexed=True)
        assert value.to_ds() == {
            "arrayValue": {
                "values": [
                    el1.to_ds(),
                    el2.to_ds(),
                    el3.to_ds(),
                ],
            },
        }

    def test_eq(self):
        a1 = ArrayValue([BooleanValue(True), NullValue()])
        a2 = ArrayValue([BooleanValue(True), NullValue()])
        assert a1 == a2

        a1 = ArrayValue([BooleanValue(True), NullValue()])
        a2 = ArrayValue([NullValue(), BooleanValue(True)])
        assert a1 != a2

    def test_setter(self):
        value = ArrayValue(None)
        assert value.py_value is None
        value.value = [NullValue(), BooleanValue(True)]
        assert value.py_value == [NullValue(), BooleanValue(True)]
        value.value = [BooleanValue(False)]
        assert value.py_value == [BooleanValue(False)]

    def test_getter(self):
        value = ArrayValue(None, raw_value={"values": []})
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        value = ArrayValue([], indexed=True)
        assert value.py_value == []
        assert value.raw_value is None
        assert value.indexed is True


class TestGeoPointValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = GeoPointValue(
            None,
            raw_value={
                "latitude": 1.23,
                "longitude": 4.56,
            },
        )
        value = value.raw_to_py()
        assert isinstance(value, LatLng)
        assert value.lat == 1.23
        assert value.lng == 4.56

    def test__py_to_raw(self):
        value = GeoPointValue(LatLng(lat=1.23, lng=4.56))
        assert value.py_to_raw() == {
            "latitude": 1.23,
            "longitude": 4.56,
        }

    def test__to_ds__raw_value(self):
        value = GeoPointValue(None, raw_value={"latitude": 1.23, "longitude": 4.56})
        assert value.to_ds() == {
            "geoPointValue": {
                "latitude": 1.23,
                "longitude": 4.56,
            },
            "excludeFromIndexes": False,
        }

    def test__to_ds__py_value(self):
        value = GeoPointValue(LatLng(lat=1.23, lng=4.56), indexed=True)
        assert value.to_ds() == {
            "geoPointValue": {
                "latitude": 1.23,
                "longitude": 4.56,
            },
            "excludeFromIndexes": False,
        }

        value = GeoPointValue(LatLng(lat=1.23, lng=4.56), indexed=False)
        assert value.to_ds() == {
            "geoPointValue": {
                "latitude": 1.23,
                "longitude": 4.56,
            },
            "excludeFromIndexes": True,
        }

    def test_eq(self):
        gp1 = GeoPointValue(LatLng(lat=1.23, lng=4.56))
        gp2 = GeoPointValue(LatLng(lat=1.23, lng=4.56))
        assert gp1 == gp2

        gp1 = GeoPointValue(LatLng(lat=1.23, lng=4.56))
        gp2 = GeoPointValue(LatLng(lat=1.23, lng=4.57))
        assert gp1 != gp2

    def test_getter(self):
        value = GeoPointValue(None, raw_value={"latitude": 1.23, "longitude": 4.56})
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        lat_lng = LatLng(lat=1.23, lng=4.56)
        value = GeoPointValue(lat_lng, indexed=False)
        assert value.py_value == lat_lng
        assert value.raw_value is None
        assert value.indexed is False


class TestBlobValue(unittest.TestCase):
    def test__raw_to_py(self):
        value = BlobValue(None, raw_value="aGVsbG8h")
        assert value.raw_to_py() == b"hello!"

    def test__py_to_raw(self):
        value = BlobValue(b"hello!")
        assert value.py_to_raw() == base64.b64encode(b"hello!").decode()

    def test__to_ds__raw_value(self):
        value = BlobValue(None, raw_value="raw-value1")
        assert value.to_ds() == {
            "blobValue": "raw-value1",
            "excludeFromIndexes": False,
        }

    def test__to_ds__py_value(self):
        value = BlobValue(b"hello!", indexed=True)
        assert value.to_ds() == {
            "blobValue": "aGVsbG8h",
            "excludeFromIndexes": False,
        }

        value = BlobValue(b"hello!", indexed=False)
        assert value.to_ds() == {
            "blobValue": "aGVsbG8h",
            "excludeFromIndexes": True,
        }

    def test_getter(self):
        value = BlobValue(None, raw_value="aGVsbG8h")
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        value = BlobValue(b"hello!", indexed=False)
        assert value.py_value == b"hello!"
        assert value.raw_value is None
        assert value.indexed is False


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
        value = KeyValue(None, raw_value=self.raw_value)
        assert value.raw_to_py() == Key.from_ds(self.raw_value)

    def test__py_to_raw(self):
        key = Key.from_ds(self.raw_value)
        value = KeyValue(key)
        assert value.py_to_raw() == self.raw_value

    def test__to_ds(self):
        key = Key.from_ds(self.raw_value)

        value = KeyValue(key, indexed=True)
        value.to_ds() == {
            "keyValue": key.to_ds(),
            "excludeFromIndexes": False,
        }

        value = KeyValue(key, indexed=False)
        value.to_ds() == {
            "keyValue": key.to_ds(),
            "excludeFromIndexes": True,
        }

    def test_getter(self):
        value = KeyValue(None, raw_value=self.raw_value)
        assert value.py_value is None
        assert value.value == value.raw_to_py()
        assert value.py_value == value.raw_to_py()

    def test_create_from_py(self):
        key = Key.from_ds(self.raw_value)
        value = KeyValue(key, indexed=False)
        assert value.py_value == key
        assert value.raw_value is None
        assert value.indexed is False
