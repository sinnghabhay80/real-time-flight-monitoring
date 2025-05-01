import time
import logging
import os
import traceback
from pyspark.sql import SparkSession, Row
from ingestion.spark_ingestion_job.utils.config_loader import load_config
from ingestion.spark_ingestion_job.utils.opensky_api_reader import OpenSkyAPIClient
from ingestion.spark_ingestion_job.utils.delta_writer import DeltaWriter
from ingestion.spark_ingestion_job.utils.schema_loader import load_schema_from_ddl, normalize_state_row

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("MainApp")

os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["HADOOP_BIN_PATH"] = "C:/hadoop/bin"

def create_spark_session(app_name: str = "RealTimeAircraftIngest") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def main():
    config = load_config()
    spark = create_spark_session()

    client = OpenSkyAPIClient()
    writer = DeltaWriter(spark)

    bronze_path = config["paths"]["bronze"]
    bronze_schema = load_schema_from_ddl("./schemas/bronze.ddl", spark)
    polling_interval = config["opensky"]["polling_interval_seconds"]
    max_retries = 0#config["opensky"].get("max_retries", 5)

    while True:
        try:
            logger.info("Polling OpenSky API...")
            states = [normalize_state_row(item, bronze_schema) for item in client.fetch_states()]
            normalize_states = [Row(*row) for row in states]

            if states:
                df = spark.createDataFrame(data=normalize_states, schema=bronze_schema)
                df.show(20)
                df.printSchema()
                writer.write_to_delta(df=df, path=bronze_path)
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
