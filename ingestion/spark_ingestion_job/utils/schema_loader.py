from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pathlib import Path


def load_schema_from_ddl(file_path: str, spark: SparkSession) -> StructType:
    ddl_str = Path(file_path).read_text()
    return spark.sql(f"SELECT {ddl_str}").schema