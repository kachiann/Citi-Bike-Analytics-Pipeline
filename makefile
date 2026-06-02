.PHONY: install build app format lint test check clean

install:
	python -m pip install -r requirements.txt

build:
	python scripts/build_warehouse.py

app:
	python -m streamlit run app/streamlit_app.py

format:
	black .
	ruff check . --fix

lint:
	ruff check .

test:
	pytest -q

check: lint test

clean:
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune -exec rm -rf {} +