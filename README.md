# Citi Bike Analytics Pipeline
Citi Bike is a public bicycle-sharing system serving New York City and parts of Jersey City. Launched in 2013, it provides short-term bike rentals through a network of docking stations and an app-based access system. It is one of the largest and most heavily used bike-share programs in the United States.

## Problem Description

Citi Bike publishes monthly trip history files containing millions of ride records. These datasets are large, time-based, and updated regularly, making them ideal for analytical workloads.

This project builds a fully reproducible batch data pipeline on GCP that:
- Ingests monthly Citi Bike trip data
- Stores raw data in a data lake (GCS)
- Processes and cleans data using Spark
- Loads optimized tables into BigQuery
- Serves analytics via a dashboard

## Business Questions
- How does ridership evolve over time?
- What is the distribution of trips by rider type (member vs casual)?
- How do electric vs classic bike usage trends change over time?
