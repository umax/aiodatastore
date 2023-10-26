[![Package version](https://badge.fury.io/py/aiodatastore.svg)](https://pypi.org/project/aiodatastore/)
[![Supported Versions](https://img.shields.io/pypi/pyversions/aiodatastore.svg)](https://pypi.org/project/aiodatastore)
[![Test](https://github.com/umax/aiodatastore/actions/workflows/dev.yml/badge.svg)](https://github.com/umax/aiodatastore/actions/workflows/dev.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# aiodatastore

__aiodatastore__ is a low level and high performance asyncio client for [Google Datastore REST API](https://cloud.google.com/datastore/docs/reference/data/rest). Inspired by [gcloud-aio](https://github.com/talkiq/gcloud-aio/blob/master/datastore) library, thanks!

Key advantages:

- lazy properties loading (that's why it's fast, mostly)

- explicit value types for properties (no types guessing)

- strictly following Google Datastore REST API data structures


## Installation

```
pip install aiodatastore
```

## How to create datastore client

```python
from aiodatastore import Datastore

client = Datastore("project1", service_file="/path/to/file")
```

You can also set namespace if needed:

```python
from aiodatastore import Datastore

client = Datastore("project1", service_file="/path/to/file", namespace="namespace1")
```

To use [Datastore emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator) (for tests or development), just define `DATASTORE_EMULATOR_HOST` environment variable (usually value is `127.0.0.1:8081`).

## How to work with [keys](https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#Key) and [entities](https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#entity)

```python
from aiodatastore import Key, PartitionId, PathElement

key = Key(PartitionId("project1"), [PathElement("Kind1")])
```

You can also set [namespace](https://cloud.google.com/datastore/docs/concepts/multitenancy) for key:
```python
from aiodatastore import Key, PartitionId, PathElement

key = Key(PartitionId("project1", namespace_id="namespace1"), [PathElement("Kind1")])
```

And `id` or `name` for path element:
```python
from aiodatastore import Key, PartitionId, PathElement

key1 = Key(PartitionId("project1"), [PathElement("Kind1", id="12345")])
key2 = Key(PartitionId("project1"), [PathElement("Kind1", name="name1")])
```

To create an entity object, you have to specify key and properties. Properties is a dict with string keys and typed values. For each [data type](https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value) the library provides corresponding value class. Every value (except ArrayValue) can be indexed or not (indexed by default):
```python
from aiodatastore import Entity, Key, PartitionId, PathElement
from aiodatastore import (
    ArrayValue,
    BoleanValue,
    BlobValue,
    DoubleValue,
    GeoPointValue,
    IntegerValue,
    LatLng,
    NullValue,
    StringValue,
    TimestampValue,
)

key = Key(PartitionId("project1"), [PathElement("Kind1")])
entity = Entity(key, properties={
    "array-prop": ArrayValue([NullValue(), IntegerValue(123), StringValue("str1")]),
    "bool-prop": BooleanValue(True),
    "blob-prop": BlobValue("data to store as blob"),
    "double-prop": DoubleValue(1.23, indexed=False),
    "geo-prop": GeoPointValue(LatLng(1.23, 4.56)),
    "integer-prop": IntegerValue(123),
    "null-prop": NullValue(),
    "string-prop": StringValue("str1"),
    "timestamp-prop": TimestampValue(datetime.datetime.utcnow()),
})
```

To access property value use `.value` attribute:
```python
print(entity.properties["integer-prop"].value)
123
```

Use `.value` attribute to change property value and keep index status. Or assign new value and set index:
```python
print(entity.properties["integer-prop"].value, entity.properties["integer-prop"].indexed)
123, True
entity.properties["integer-prop"].value = 456
print(entity.properties["integer-prop"].value, entity.properties["integer-prop"].indexed)
456, True

entity.properties["integer-prop"] = IntegerValue(456, indexed=True)
print(entity.properties["integer-prop"].value, entity.properties["integer-prop"].indexed)
456, True
```

Use `.indexed` attribute to access or change index:
```python
print(entity.properties["integer-prop"].indexed)
True

entity.properties["integer-prop"].indexed = False
print(entity.properties["integer-prop"].indexed)
False
```

To insert new entity (the entity key's final path element may be incomplete):
```python
key = Key(PartitionId("project1"), [PathElement("Kind1")])
entity = Entity(key, properties={
    "string-prop": StringValue("some value"),
})
await client.insert(entity)
```

To update an entity (the entity must already exist. Must have a complete key path):
```python
entity.properties["string-prop"] = StringValue("new value")
await client.update(entity)
```

To upsert an entity (the entity may or may not already exist. The entity key's final path element may be incomplete):
```python
key = Key(PartitionId("project1"), [PathElement("Kind1")])
entity = Entity(key, properties={
    "string-prop": StringValue("some value"),
})
await client.upsert(entity)
```

To delete an entity (the entity may or may not already exist. Must have a complete key path and must not be reserved/read-only):
```python
await client.delete(entity)
```

If you have entity's key or know how to build it:
```python
await client.delete(key)
````
