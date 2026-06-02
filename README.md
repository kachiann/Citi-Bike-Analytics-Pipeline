# Citi Bike Analytics Pipeline

End-to-end analytics engineering project for NYC Citi Bike trip data, implemented with a cloud-first GCP architecture and a local DuckDB development mode.

## Project goal

This project transforms monthly Citi Bike trip history data into an analytics-ready warehouse and an interactive dashboard for time-series ridership analysis. It is designed to demonstrate production-style batch data engineering patterns across cloud storage, warehousing, SQL transformation, and analytics delivery.

## Architectures

### Cloud architecture

Citi Bike monthly CSV files  
→ Google Cloud Storage (raw data lake)  
→ BigQuery (raw → staging → marts)  
→ partitioned and clustered fact tables  
→ Streamlit dashboard on Cloud Run

### Local development mode

Citi Bike monthly CSV files  
→ local raw storage  
→ DuckDB (raw → staging → marts)  
→ Streamlit dashboard

The local mode makes it easy to iterate on SQL transformations and dashboard logic without provisioning cloud resources for every development cycle.

## Tech stack

### Cloud
- Google Cloud Storage (GCS)
- BigQuery
- Cloud Run
- Terraform

### Local / analytics
- DuckDB
- SQL
- Python
- Streamlit
- Plotly

## Data model

The warehouse follows a layered ELT pattern:

### raw
Landed source trip files with minimal modification.

### staging
Cleaned and standardized trip records with typed timestamps and derived analytical columns such as:
- `ride_date`
- `ride_month`
- `ride_dow`
- `day_type`
- `trip_duration_min`

### marts
Business-ready analytical tables optimized for dashboard queries.

Current mart:
- `fact_daily_trips`

## Warehouse design choices

### BigQuery
In the cloud warehouse, analytical fact tables are designed to:
- partition by `ride_date` for time-series pruning and reduced scan costs
- cluster by frequently filtered dashboard dimensions such as `member_casual` and `rideable_type`

Partitioning by the primary time dimension and clustering by commonly filtered columns is a standard BigQuery optimization pattern for large analytical workloads.

### DuckDB
In local mode, DuckDB provides a lightweight analytical engine for:
- reading multiple monthly CSV files
- iterating on SQL transformations locally
- supporting dashboard development without cloud dependencies

DuckDB is a strong fit for local analytics workflows because it supports modern analytical SQL and efficient local querying over large datasets.

## Business questions

- How does Citi Bike ridership evolve over time?
- How is ridership distributed between member and casual riders?
- How do electric and classic bike usage patterns differ?
- How does weekday usage compare with weekend usage?

## How to run locally

### Prerequisites
- Python 3.10+
- Make
- Citi Bike source files placed in `data/raw/`

### Setup
```bash
make install
make build
make app
```

## How to run on GCP

### Prerequisites
- Google Cloud SDK
- Terraform
- Application Default Credentials configured
- A GCP project with billing enabled

### Provision infrastructure
```bash
cd terraform
terraform init
terraform apply
```

### Cloud components
- GCS bucket for raw trip files
- BigQuery dataset for analytics tables
- optional Cloud Run deployment for the Streamlit dashboard

Cloud Run is a common GCP target for containerized Streamlit apps and works well for lightweight analytics dashboards.

## Notes

- Local raw data and DuckDB files are gitignored.
- The local analytical mart currently covers the subset of 2024 data loaded into the repository workflow.
- Raw and staging layers preserve source truth, while marts are scoped for analytical consistency.

## Dashboard

![Citi Bike dashboard](https://github.com/user-attachments/assets/201bd51e-639b-4f63-96c8-cbc8a557213a)
