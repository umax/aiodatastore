import dataclasses
import sys
from functools import partial

__all__ = ("dataclass",)


dataclass = dataclasses.dataclass
if sys.version_info[1] >= 10:
    dataclass = partial(dataclass, slots=True)
