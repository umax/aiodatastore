.PHONY: black black-check build flake8 mypy test-integration test-unit


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

rundatastore:
	gcloud beta emulators datastore start --no-store-on-disk --project=test --consistency=1.0

test-integration:
	DATASTORE_EMULATOR_HOST=127.0.0.1:8081 python -m pytest -v --cov-report=term tests/integration

test-unit:
	python -m pytest -v --cov-report=term-missing --cov=aiodatastore tests/unit
