import time
import logging
from pyspark.sql import SparkSession, Row
from utils.config_loader import load_config
from utils.opensky_api_reader import OpenSkyAPIClient
from utils.delta_writer import DeltaWriter
from utils.schema_loader import load_schema_from_ddl, normalize_state_row
from utils.spark_session import create_spark_session

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("BronzeIngestionLayer")


def main():
    config = load_config()
    spark = create_spark_session(app_name="RealTimeAircraftIngest")

    client = OpenSkyAPIClient()
    writer = DeltaWriter(spark)

    bronze_delta_path = config["paths"]["bronze"]
    bronze_schema_path = config["paths"]["bronze_schema"]
    bronze_schema = load_schema_from_ddl(bronze_schema_path, spark)
    polling_interval = config["opensky"]["polling_interval_seconds"]

    while True:
        try:
            logger.info("Polling OpenSky API...")
            states = [normalize_state_row(item, bronze_schema) for item in client.fetch_states()]
            normalize_states = [Row(*row) for row in states]

            if states:
                df = spark.createDataFrame(data=normalize_states, schema=bronze_schema)
                writer.write_to_delta(df=df, path=bronze_delta_path)
                logger.info(f"Written {df.count()} records to Delta.")
            else:
                logger.warning("No data received from OpenSky API.")

        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            break

        logger.info(f"Sleeping for {polling_interval} seconds before next poll...")
        time.sleep(polling_interval)

if __name__ == "__main__":
    main()