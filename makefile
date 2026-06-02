.PHONY: install build app

install:
	python -m pip install -r requirements.txt

build:
	python scripts/build_warehouse.py

app:
	python -m streamlit run app/streamlit_app.py