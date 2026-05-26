from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Keep this DAG lightweight and repo-friendly.
# It assumes:
# - your Dataproc cluster already exists
# - the job artifacts are stored in GCS
# - prod.yaml is already uploaded to gs://syem-genai-artifacts/configs/prod.yaml

PROJECT_ID = "syem-genai-data-platform"
REGION = "asia-south1"
CLUSTER = "genai-cluster"
ARTIFACT_BUCKET = "gs://syem-genai-artifacts"
CONFIG_URI = f"{ARTIFACT_BUCKET}/configs/prod.yaml"
JOBS_URI = f"{ARTIFACT_BUCKET}/jobs"

DEFAULT_ARGS = {
    "owner": "syed",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def submit_job(job_file: str, task_id: str) -> BashOperator:
    """Build a Dataproc submit command for a single PySpark job."""
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"gcloud dataproc jobs submit pyspark "
            f"{JOBS_URI}/{job_file} "
            f"--cluster={CLUSTER} "
            f"--region={REGION} "
            f"--project={PROJECT_ID} "
            f"-- --config {CONFIG_URI}"
        ),
    )


with DAG(
    dag_id="olist_end_to_end_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end Olist pipeline: process -> curated -> marts",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "dataproc", "spark", "bigquery", "analytics"],
) as dag:
    # Process layer
    process_orders = submit_job("process_orders.py", "process_orders")
    process_order_items = submit_job("process_order_items.py", "process_order_items")
    process_customers = submit_job("process_customers.py", "process_customers")
    process_products = submit_job("process_products.py", "process_products")
    process_sellers = submit_job("process_sellers.py", "process_sellers")
    process_payments = submit_job("process_payments.py", "process_payments")
    process_reviews = submit_job("process_reviews.py", "process_reviews")
    process_category_translation = submit_job(
        "process_category_translation.py",
        "process_category_translation",
    )
    process_geolocation = submit_job("process_geolocation.py", "process_geolocation")

    # Curated layer
    curate_orders = submit_job("curated/curate_orders.py", "curate_orders")
    curate_order_items = submit_job("curated/curate_order_items.py", "curate_order_items")
    curate_customers = submit_job("curated/curate_customers.py", "curate_customers")
    curate_products = submit_job("curated/curate_products.py", "curate_products")
    curate_sellers = submit_job("curated/curate_sellers.py", "curate_sellers")
    curate_payments = submit_job("curated/curate_payments.py", "curate_payments")
    curate_reviews = submit_job("curated/curate_reviews.py", "curate_reviews")
    curate_category_translation = submit_job(
        "curated/curate_category_translation.py",
        "curate_category_translation",
    )
    curate_geolocation = submit_job("curated/curate_geolocation.py", "curate_geolocation")

    # Mart layer
    seller_performance = submit_job("marts/seller_performance.py", "seller_performance_mart")
    customer_rfm = submit_job("marts/customer_rfm.py", "customer_rfm_mart")
    daily_sales_mart = submit_job("marts/daily_sales_mart.py", "daily_sales_mart")

    # Flow
    [
        process_orders,
        process_order_items,
        process_customers,
        process_products,
        process_sellers,
        process_payments,
        process_reviews,
        process_category_translation,
        process_geolocation,
    ] >> [
        curate_orders,
        curate_order_items,
        curate_customers,
        curate_products,
        curate_sellers,
        curate_payments,
        curate_reviews,
        curate_category_translation,
        curate_geolocation,
    ] >> [
        seller_performance,
        customer_rfm,
        daily_sales_mart,
    ]

