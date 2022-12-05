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
