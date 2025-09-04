#!/bin/bash

set -e

ruff check --fix src test
ruff format src test
mypy --pretty src
sqlfluff fix --show-lint-violations src/core/crud/sql/