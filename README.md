# Citi Bike Analytics Pipeline
End-to-end cloud data engineering project built on Google Cloud Platform (GCP), processing 44+ million NYC Citi Bike trips (2024) into a scalable analytics warehouse and interactive dashboard.

## Project Overview

Citi Bike is one of the largest public bike-sharing systems in the United States. It publishes monthly trip history datasets containing millions of ride records.

These datasets are:
- Large (millions of rows per month)
- Time-series based
- Updated regularly
- Ideal for analytical workloads

This project demonstrates how to design and implement a production-style batch data pipeline in the cloud — from raw data ingestion to business-ready analytics.

## Architecture

```
Citi Bike Monthly Data (CSV)
        ↓
Google Cloud Storage (Data Lake)
        ↓
BigQuery (raw → staging → marts)
        ↓
Partitioned & Clustered fact_trips table
        ↓
Streamlit Analytics Dashboard
```
## Tech Stack
- Cloud: Google Cloud Platform (GCP)
- Infrastructure as Code: Terraform
- Data Lake: Google Cloud Storage (GCS)
- Data Warehouse: BigQuery (Partitioned & Clustered)
- Data Processing: SQL (ELT architecture)
- Analytics Layer: Streamlit

## Business Questions Answered
- How does ridership evolve over time?
- What is the distribution of trips by rider type?
- How do electric vs classic bike trends change?
- How does usage vary across weekdays and weekends?


## How to run Locally
### Installation

1. Clone this repository
```bash
git clone https://github.com/kachiann/Citi-Bike-Analytics-Pipeline.git
cd Citi-Bike-Analytics-Pipeline
```
2. Go to folder
```bash
cd terraform
```
Ensure:
- `gcloud` is installed
- Application Default Credentials are configured

3. Run
   
   ```terraform init```

   ```terraform apply```

## Usage

Run the Streamlit app:
```bash
streamlit run streamlit_app.py
```
The app will open in your default web browser.

