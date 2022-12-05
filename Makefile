.PHONY: init black flake8 unittest

init:
	pip install -r requirements.txt

black:
	black aiodatastore tests

flake8:
	flake8 aiodatastore tests

unittest:
	python -m pytest -v --cov aiodatastore tests
