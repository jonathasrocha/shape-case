from pyspark.sql import SparkSession


def create_tables(
    spark,
    path="s3a://equipment/delta/",
    database: str = "equipment",
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    spark.sql(f"DROP TABLE IF EXISTS {database}.equipment")
    spark.sql(
        f"""
            CREATE TABLE {database}.equipment (
                equipment_id INT,
                name STRING,
                group_name STRING,
                updated_at_dt TIMESTAMP,
                partition STRING
            ) USING DELTA
            PARTITIONED BY (partition)
            LOCATION '{path}/equipment/equipment/'
        """
    )

    spark.sql(f"DROP TABLE IF EXISTS {database}.equipment_sensors")
    spark.sql(
        f"""
            CREATE TABLE {database}.equipment_sensors (
                equipment_id STRING,
                sensor_id STRING,
                partition STRING
            ) USING DELTA
            PARTITIONED BY (partition)
            LOCATION '{path}/equipment/equipment_sensors/'
        """
    )

    spark.sql(
        f"""
            CREATE TABLE {database}.equipment_failure_sensors (
                value STRING,
                partition STRING
            ) USING DELTA
            PARTITIONED BY (partition)
            LOCATION '{path}/equipment/equipment_sensors/'
        """
    )

def drop_tables(
    spark,
    database: str = "equipment",
):
    spark.sql(f"DROP TABLE IF EXISTS {database}.equipment")
    spark.sql(f"DROP TABLE IF EXISTS {database}.equipment_sensors")
    spark.sql(f"DROP DATABASE IF EXISTS {database}.equipment_failure_sensors")


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("{database}_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)