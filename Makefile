.PHONY: install demo test lint dry-run
install:
	python -m pip install -e ".[dev]"
demo:
	pipelineco demo
test:
	python -m pytest
lint:
	ruff check src tests dags
dry-run:
	pipelineco generate && pipelineco run --dry-run
