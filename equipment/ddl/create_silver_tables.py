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
                sensor_id STRING,
                log_level STRING,
                created_at_dt timestamp,
                equipment_id STRING,
                updated_at_dt TIMESTAMP,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP
            LOCATION '{path}/equipment/dim_equipment'
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