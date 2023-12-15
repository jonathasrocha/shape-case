from pyspark.sql import SparkSession


def create_tables(
    spark,
    path="s3a://equipment/delta/gold",
    database: str = "equipment"
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    spark.sql(f"DROP TABLE IF EXISTS {database}.equipment_failure_mart")
    spark.sql(
        f"""
            CREATE TABLE {database}.equipment_failure_mart (
                equipment_sk STRING,
                equipment_id STRING,
                sensor_id STRING,
                log_level STRING,
                created_at_dt DATE,
                equipment_name STRING,
                equipment_group STRING,
                count INTEGER,
                avg_temperature DECIMAL(18,2),
                avg_vibration DECIMAL(18,2),
                etl_inserted TIMESTAMP,
                partition STRING
            ) USING DELTA
            PARTITIONED BY (partition)
            LOCATION '{path}/equipment/equipment_failure_mart'
        """
    )

def drop_tables(
    spark,
    database: str = "equipment"
):
    spark.sql(f"DROP DATABASE IF EXISTS {database}.equipment_failure_mart")


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("{database}_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)
