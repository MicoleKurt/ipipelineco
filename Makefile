.PHONY: install demo test lint dry-run
install:
	python -m pip install -e ".[dev]"
demo:
	ipipelineco demo
test:
	python -m pytest
lint:
	ruff check src tests dags
dry-run:
	ipipelineco generate && ipipelineco run --dry-run
