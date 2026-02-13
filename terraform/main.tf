provider "google" {
  project = var.project_id
  region  = var.region
}

# -------------------------
# GCS bucket (Data Lake)
# -------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = var.bucket_name
  location                    = var.location
  uniform_bucket_level_access = true
  force_destroy               = true
}

# -------------------------
# BigQuery datasets
# -------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = var.location
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "staging"
  location   = var.location
}

resource "google_bigquery_dataset" "marts" {
  dataset_id = "marts"
  location   = var.location
}
