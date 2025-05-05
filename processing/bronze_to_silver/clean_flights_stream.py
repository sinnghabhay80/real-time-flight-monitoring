import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from ingestion.spark_ingestion_job.utils.config_loader import load_config
from ingestion.spark_ingestion_job.utils.spark_session import create_spark_session
from ingestion.spark_ingestion_job.utils.streaming_query_listener import MyQueryListener

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("SilverStreamingLayer")


def clean_data(df: DataFrame) -> DataFrame:
    """
    Applies a set of cleaning transformations to the raw flight data:
    - Drops rows with nulls in critical fields
    - Filters out invalid coordinates and altitudes
    - Removes entries with missing or invalid ICAO codes
    - Deduplicates based on aircraft ID and timestamps
    - Adds ingestion timestamp
    """
    cleaned_df = df\
                 .dropna(subset=["icao24", "time_position", "latitude", "longitude"])\
                 .filter((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)))\
                 .filter((col("baro_altitude").isNull()) | (col("baro_altitude") >= 0))\
                 .filter((col("icao24").isNotNull()) & (col("icao24") != "") & (col("icao24").rlike("^[a-fA-F0-9]{6}$"))) \
                 .dropDuplicates(subset=["icao24", "time_position", "last_contact"])\
                 .withColumn("ingestion_timestamp", current_timestamp())

    return cleaned_df


def main():
    config = load_config()
    spark = create_spark_session(app_name="CleanFlightsStream")
    spark.streams.addListener(MyQueryListener())

    bronze_delta_path = config["paths"]["bronze"]

    silver_delta_path = config["paths"]["silver"]

    logger.info("Reading Bronze table as streaming source...")
    bronze_df = spark\
                .readStream\
                .format("delta")\
                .load(bronze_delta_path)

    logger.info("Applying cleaning transformations...")
    silver_df = clean_data(bronze_df)

    logger.info("Writing cleaned data to Silver Delta table...")
    query = silver_df\
            .writeStream\
            .format("delta")\
            .outputMode("append")\
            .trigger(processingTime="10 seconds")\
            .option("maxBytesPerTrigger", "10000000")\
            .option("checkpointLocation", config["paths"]["silver_checkpoint"])\
            .start(silver_delta_path)

    query.awaitTermination()


if __name__ == "__main__":
    main()