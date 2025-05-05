from pyspark.sql import DataFrame, SparkSession
import logging
from typing import Optional, List
from ingestion.spark_ingestion_job.utils.config_loader import load_config

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("DeltaWriter")


class DeltaWriter:
    def __init__(self, spark: SparkSession, config_path: str = "/home/abhays/real-time-flight-monitoring/ingestion/spark_ingestion_job/config/config.yaml"):
        self.spark = spark
        self.config = load_config(config_path)
        self.default_mode = self.config["delta"].get("mode", "append")
        self.default_merge_schema = self.config["delta"].get("merge_schema", False)

    def write_to_delta(
        self,
        df: DataFrame,
        path: str,
        mode: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        merge_schema: Optional[bool] = None
    ):
        """
        Generic Delta Lake writer.
        """
        try:
            writer = df.write.format("delta").mode(mode or self.default_mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            if (merge_schema if merge_schema is not None else self.default_merge_schema) and (mode or self.default_mode) == "overwrite":
                writer = writer.option("mergeSchema", "true")

            writer.save(path)
            logger.info(f"Data written to Delta at {path} (mode={mode or self.default_mode}).")

        except Exception as e:
            logger.error(f"Failed to write to Delta Lake at {path}: {e}")
            raise