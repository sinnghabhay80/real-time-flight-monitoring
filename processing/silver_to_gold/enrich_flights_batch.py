import logging
from pyspark.sql.functions import col, to_date, count, sum, avg, max, min, expr, when, from_unixtime, hour, countDistinct, sqrt
from utils.config_loader import load_config
from utils.delta_writer import DeltaWriter
from utils.spark_session import create_spark_session

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("BatchGoldLayer")

def main():
    spark = create_spark_session("EnrichFlightsBatch")
    config = load_config()
    writer = DeltaWriter(spark)

    silver_delta_path = config["paths"]["silver"]
    silver_schema = config["paths"]["silver_schema"]
    gold_delta_path = config["paths"]["gold"]

    logger.info("Reading flight data from Silver Delta table...")
    silver_df = spark\
                .read\
                .format("delta")\
                .load(silver_delta_path)

    logger.info("Applying Enrichment Columns...")
    df_enchriched = silver_df\
                    .withColumn("horizontal_speed", sqrt(col("velocity")**2 - col("vertical_rate")**2))\
                    .withColumn("timestamp_utc", from_unixtime(col("last_contact")))\
                    .withColumn("date", to_date(col("timestamp_utc")))\
                    .withColumn("hour", hour(col("timestamp_utc")))\
                    .withColumn("flight_phase", when(col("vertical_rate") > 1, "climbing")\
                                                        .when(col("vertical_rate") < -1, "descending")\
                                                        .otherwise("level"))\
                    .withColumn("is_high_altitude", col("baro_altitude") > 10000)\
                    .withColumn("is_fast", col("velocity") > 250)

    logger.info("Applying Aggregations...")
    df_agg = df_enchriched\
             .groupby(col("origin_country"), col("date"), col("hour"))\
             .agg(
                count("*").alias("total_flights"),
                sum(when(col("is_high_altitude"), 1).otherwise(0)).alias("high_altitude_flights"),
                sum(when(col("is_fast"), 1).otherwise(0)).alias("fast_flights"),
                avg(col("horizontal_speed")).alias("avg_horizontal_speed"),
                avg(col("velocity")).alias("avg_velocity"),
                avg(col("baro_altitude")).alias("avg_altitude"),
                avg(col("vertical_rate")).alias("avg_vertical_rate"),
                max(col("baro_altitude")).alias("max_altitude"),
                min(col("baro_altitude")).alias("min_altitude"),
                countDistinct(col("flight_phase")).alias("flight_phases_detected"),
                expr("count_if(flight_phase = 'climbing')").alias("climbing_flights"),
                expr("count_if(flight_phase = 'descending')").alias("descending_flights"),
                expr("count_if(flight_phase = 'level')").alias("level_flights")
             )

    logger.info("Writing Aggregated Data to Gold Delta table...")
    writer.write_to_delta(df = df_agg,
                          path = gold_delta_path,
                          mode = "overwrite",
                          partition_by=["date", "hour"])


if __name__ == "__main__":
    main()