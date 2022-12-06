.PHONY: black black-check flake8 mypy unittest

SOURCE_FILES = aiodatastore tests


black:
	black $(SOURCE_FILES)

black-check:
	black --diff --check $(SOURCE_FILES)

flake8:
	flake8 $(SOURCE_FILES)

mypy:
	mypy aiodatastore

unittest:
	python -m pytest -v --cov-report=term-missing --cov=aiodatastore tests/unit
