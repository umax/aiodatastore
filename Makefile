.PHONY: init black flake8 unittest

init:
	pip install -r requirements.txt

black:
	black --line-length=79 -t py38 -t py39 -t py310 -t py311 .

flake8:
	flake8 aiodatastore tests

unittest:
	python -m pytest -v --cov aiodatastore tests/unit
