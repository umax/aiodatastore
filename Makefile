.PHONY: black flake8

black:
	black aiodatastore tests

flake8:
	flake8 aiodatastore tests
