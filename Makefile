.PHONY: black black-check build flake8 mypy publish pylint test-integration test-unit test


black:
	black aiodatastore tests

black-check:
	black --diff --check aiodatastore tests

build:
	rm -rf dist/* && python -m build

flake8:
	flake8 aiodatastore tests

mypy:
	mypy aiodatastore

publish:
	twine upload dist/*

pylint:
	pylint --disable=C0114,C0115,C0116,R0902,R0903,R0913 aiodatastore

rundatastore:
	gcloud beta emulators datastore start --no-store-on-disk --project=test --consistency=1.0

test-integration:
	DATASTORE_EMULATOR_HOST=127.0.0.1:8081 python -m pytest -v --cov-report=term --cov=aiodatastore tests/integration

test-unit:
	python -m pytest -v --cov-report=term-missing --cov=aiodatastore tests/unit

test: test-unit test-integration
