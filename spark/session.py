from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Retail Data Platform")
    .getOrCreate()
)