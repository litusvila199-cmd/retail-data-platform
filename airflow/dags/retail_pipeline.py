from datetime import timedelta
from airflow.sdk import dag, task

from src.ingestion.extract import extract_table 
from src.transforms.process import process_table
from src.generators.sales import generate_sales
from src.generators.customer_analytics import generate_customer_analytics
from src.config.tables import TABLES

@dag(
    dag_id="retail_pipeline",
    schedule="@daily",
    catchup=False,
)

def retail_pipeline():

    @task(
        retries=2,
        retry_delay=timedelta(seconds=10)
    )
    def extract():
        for table in TABLES:
            extract_table(table)

    @task(
            retries=2,
            retry_delay=timedelta(seconds=10)
    )
    def process():
        for table in TABLES:
            process_table(table)

    @task(
            retries=2,
            retry_delay=timedelta(seconds=10)
    )
    def sales():
        generate_sales()

    @task(
            retries=2,
            retry_delay=timedelta(seconds=10)
    )
    def customer_analytics():
        generate_customer_analytics()


    extract_task = extract()
    process_task = process()
    sales_task = sales()
    customer_analytics_task = customer_analytics()

    extract_task >> process_task >> sales_task >> customer_analytics_task 

retail_pipeline()               