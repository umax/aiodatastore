.PHONY: init black black-check flake8 unittest

init:
	pip install -r requirements.txt

black:
	black .

black-check:
	black --diff --check .

flake8:
	flake8 aiodatastore tests

unittest:
	python -m pytest -v --cov aiodatastore tests/unit
