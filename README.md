[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# aiodatastore

__aiodatastore__ is a low level and high performance asyncio client for [Google Datastore](https://cloud.google.com/datastore). Inspired by [gcloud-aio](https://github.com/talkiq/gcloud-aio/blob/master/datastore) library, thanks!

Key advantages:

- lazy properties loading (that's why it fast, mostly)

- explicit value types for properties (no types guessing)

- strictly following REST API data structures
