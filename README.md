[![Package version](https://badge.fury.io/py/aiodatastore.svg)](https://pypi.org/project/aiodatastore/)
[![Supported Versions](https://img.shields.io/pypi/pyversions/aiodatastore.svg)](https://pypi.org/project/aiodatastore)
[![Test](https://github.com/umax/aiodatastore/actions/workflows/test.yml/badge.svg)](https://github.com/umax/aiodatastore/actions/workflows/test.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# aiodatastore

__aiodatastore__ is a low level and high performance asyncio client for [Google Datastore REST API](https://cloud.google.com/datastore/docs/reference/data/rest). Inspired by [gcloud-aio](https://github.com/talkiq/gcloud-aio/blob/master/datastore) library, thanks!

Key advantages:

- lazy properties loading (that's why it fast, mostly)

- explicit value types for properties (no types guessing)

- strictly following REST API data structures


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

## How to create [keys](https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#Key) and [entities](https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#entity)

```python
from aiodatastore import Key, PartitionId, PathElement

key = Key(PartitionId("project1"), [PathElement("Kind1")])
```

You can also set namespace for key:
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
