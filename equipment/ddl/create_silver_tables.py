from pyspark.sql import SparkSession


def create_tables(
    spark,
    path="s3a://equipment/delta/silver",
    database: str = "equipment",
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_equipment")
    spark.sql(
        f"""
            CREATE TABLE {database}.dim_equipment (
                equipment_sk STRING,
                equipment_id STRING,
                name STRING,
                group_name STRING,
                updated_at_dt TIMESTAMP,
                current BOOLEAN,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP
            ) USING DELTA
            LOCATION '{path}/equipment/dim_equipment'
        """
    )
    spark.sql(f"DROP TABLE IF EXISTS {database}.equipment_failure_sensors")
    spark.sql(
        f"""
            CREATE TABLE {database}.equipment_failure_sensors (
                created_at_dt TIMESTAMP,
                log_level STRING,
                sensor_id STRING,
                temperature DECIMAL(18,2),
                vibration DECIMAL(18,2),
                etl_inserted TIMESTAMP,
                partition STRING
            ) USING DELTA
            LOCATION '{path}/equipment/equipment_failure_sensors'
        """
    )
   
def drop_tables(
    spark,
    database: str = "equipment",
):
    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_equipment")


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("{database}_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)