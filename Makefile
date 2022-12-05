.PHONY: init black black-check flake8 unittest

SOURCE_FILES = aiodatastore tests

init:
	pip install -r requirements.txt

black:
	black $(SOURCE_FILES)

black-check:
	black --diff --check $(SOURCE_FILES)

flake8:
	flake8 $(SOURCE_FILES)

unittest:
	python -m pytest -v --cov=aiodatastore tests/unit
