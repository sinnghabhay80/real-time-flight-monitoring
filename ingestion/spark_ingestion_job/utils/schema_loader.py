from pathlib import Path
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DoubleType, IntegerType, StructType

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("SchemaLoader")


def load_schema_from_ddl(file_path: str, spark: SparkSession) -> StructType:
    ddl_str = Path(file_path).read_text()
    return ddl_to_schema(spark, ddl_str)


def ddl_to_schema(spark: SparkSession, ddl_str: str) -> StructType:
    empty_rdd = spark.sparkContext.emptyRDD()
    return spark.read.schema(ddl_str).json(empty_rdd).schema


def normalize_state_row(item: list, schema: StructType) -> list:
    """
    Normalize raw list values based on the expected Spark schema types.
    """
    normalized = []
    for val, field in zip(item, schema.fields):
        try:
            if val is None:
                normalized.append(None)
            elif isinstance(field.dataType, FloatType):
                normalized.append(float(val))
            elif isinstance(field.dataType, DoubleType):
                normalized.append(float(val))
            elif isinstance(field.dataType, IntegerType):
                normalized.append(int(val))
            else:
                normalized.append(val)
        except Exception as e:
            logger.warning(f"Value '{val}' could not be cast to {field.dataType}: {e}")
            normalized.append(None)
    return normalized