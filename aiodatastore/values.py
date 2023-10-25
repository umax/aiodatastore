from base64 import b64decode, b64encode
from datetime import datetime
from typing import Any, Optional

from aiodatastore.key import Key

__all__ = (
    "NullValue",
    "BooleanValue",
    "StringValue",
    "IntegerValue",
    "DoubleValue",
    "TimestampValue",
    "BlobValue",
    "ArrayValue",
    "LatLng",
    "GeoPointValue",
    "KeyValue",
)


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value
class Value:
    __slots__ = ("py_value", "raw_value", "indexed")

    type_name: str

    def __init__(
        self, value: Any, raw_value: Any = None, indexed: Optional[bool] = True
    ):
        self.py_value = value  # initialized manually on new property definition
        self.raw_value = raw_value  # initialized on parsing response from datastore
        self.indexed = indexed

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.value == other.value

    @property
    def value(self):
        if self.py_value is not None:
            return self.py_value

        self.py_value = value = self.raw_to_py()
        return value

    @value.setter
    def value(self, value):
        self.py_value = value

    def to_ds(self):
        if self.py_value is not None:
            raw_value = self.py_to_raw()
        else:
            raw_value = self.raw_value

        return {
            self.type_name: raw_value,
            "excludeFromIndexes": not self.indexed,
        }

    def raw_to_py(self):
        raise NotImplementedError

    def py_to_raw(self):
        raise NotImplementedError


class NullValue(Value):
    type_name = "nullValue"

    def __init__(self, indexed: Optional[bool] = True):
        super().__init__(None, raw_value="NULL_VALUE", indexed=indexed)

    def raw_to_py(self):
        return None

    def py_to_raw(self):
        return "NULL_VALUE"


class BooleanValue(Value):
    type_name = "booleanValue"

    def raw_to_py(self):
        return self.raw_value

    def py_to_raw(self):
        return self.py_value


class StringValue(Value):
    type_name = "stringValue"

    def raw_to_py(self):
        return self.raw_value

    def py_to_raw(self):
        return self.py_value


class IntegerValue(Value):
    type_name = "integerValue"

    def raw_to_py(self):
        return int(self.raw_value)

    def py_to_raw(self):
        return str(self.py_value)


class DoubleValue(Value):
    type_name = "doubleValue"

    def raw_to_py(self):
        return float(self.raw_value)

    def py_to_raw(self):
        return self.py_value


class TimestampValue(Value):
    type_name = "timestampValue"

    def raw_to_py(self):
        return datetime.fromisoformat(self.raw_value[:26].replace("Z", ""))

    def py_to_raw(self):
        # A timestamp in RFC3339 UTC "Zulu" format, with nanosecond
        # resolution and up to nine fractional digits.
        return datetime.isoformat(self.py_value)[:26] + "Z"


class BlobValue(Value):
    type_name = "blobValue"

    def raw_to_py(self):
        return b64decode(self.raw_value)

    def py_to_raw(self):
        return b64encode(self.py_value).decode()


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/ArrayValue
class ArrayValue(Value):
    type_name = "arrayValue"

    def raw_to_py(self):
        result = []
        for el in self.raw_value["values"]:
            for key in el:
                if key.endswith("Value"):
                    break
            else:
                raise RuntimeError(f'unsupported type of "{el}" array element')

            value_type = VALUE_TYPES[key]
            if value_type is NullValue:
                _value = NullValue(indexed=not el.get("excludeFromIndexes"))
            else:
                _value = value_type(
                    None,
                    raw_value=el[key],
                    indexed=not el.get("excludeFromIndexes"),
                )
            result.append(_value)

        return result

    def py_to_raw(self):
        return [v.to_ds() for v in self.py_value]

    def to_ds(self):
        if self.py_value is not None:
            raw_value = self.py_to_raw()
        else:
            raw_value = self.raw_value["values"]

        return {self.type_name: {"values": raw_value}}


# https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/LatLng
class LatLng:
    __slots__ = ("lat", "lng")

    def __init__(self, lat: float, lng: float) -> None:
        self.lat = lat
        self.lng = lng

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, LatLng)
            and self.lat == other.lat
            and self.lng == other.lng
        )


class GeoPointValue(Value):
    type_name = "geoPointValue"

    def raw_to_py(self):
        return LatLng(
            lat=float(self.raw_value["latitude"]),
            lng=float(self.raw_value["longitude"]),
        )

    def py_to_raw(self):
        return {
            "latitude": self.value.lat,
            "longitude": self.value.lng,
        }


class KeyValue(Value):
    type_name = "keyValue"

    def raw_to_py(self):
        return Key.from_ds(self.raw_value)

    def py_to_raw(self):
        return self.py_value.to_ds()


VALUE_TYPES = {
    NullValue.type_name: NullValue,
    BooleanValue.type_name: BooleanValue,
    StringValue.type_name: StringValue,
    IntegerValue.type_name: IntegerValue,
    DoubleValue.type_name: DoubleValue,
    TimestampValue.type_name: TimestampValue,
    BlobValue.type_name: BlobValue,
    ArrayValue.type_name: ArrayValue,
    GeoPointValue.type_name: GeoPointValue,
    KeyValue.type_name: KeyValue,
}
