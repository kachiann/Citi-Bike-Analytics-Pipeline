# Citi Bike Analytics Pipeline
Citi Bike is a public bicycle-sharing system serving New York City and parts of Jersey City. Launched in 2013, it provides short-term bike rentals through a network of docking stations and an app-based access system. It is one of the largest and most heavily used bike-share programs in the United States.

## Problem Description

Citi Bike publishes monthly trip history files containing millions of ride records. These datasets are large, time-based, and updated regularly, making them ideal for analytical workloads. In particular, this is an end-to-end cloud data engineering project built on GCP, processing 44M+ NYC Citi Bike trips (2024) into an optimized analytics warehouse and interactive dashboard.

## Tech Stack
- Cloud: Google Cloud Platform (GCP)
- Infrastructure as Code: Terraform
- Data Lake: Google Cloud Storage (GCS)
- Data Warehouse: BigQuery (Partitioned & Clustered)
- Transformations: SQL (ELT pattern)
- Dashboard: Streamlit

## Architecture

```
Citi Bike Monthly Data (CSV)
        ↓
GCS (Data Lake)
        ↓
BigQuery (raw → staging → marts)
        ↓
Partitioned fact_trips table
        ↓
Streamlit Analytics Dashboard
```


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
```
2. Go to folder
```bash
cd terraform
```
3. Run
   
   ```terraform init```

   ```terraform apply```

## Usage

Run the Streamlit app:
```bash
streamlit run streamlit_app.py
```
The app will open in your default web browser.

