from pyspark.sql import SparkSession
import pytest
from delta import configure_spark_with_delta_pip
from equipment.pipeline.equipment import EquipmentETL


@pytest.fixture
def spark():
    my_packages = [
        "io.delta:delta-core_2.12:2.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.2",
    ]
     

    builder = (
        SparkSession.builder.appName("equipment")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
    )

    return configure_spark_with_delta_pip(
        builder, extra_packages=my_packages
    ).getOrCreate()
    

def test_equipment(spark):
    

    df_input = spark.createDataFrame(
        [
            ["[2021-05-18 0:20:48]	ERROR	sensor[5820]:	(temperature	311.29, vibration	6749.50)[2021-05-18 0:20:48]	ERROR	sensor[5820]:	(temperature	311.29, vibration	6749.50)"]
        ],
        ["value"]
    )
    equip = EquipmentETL()


    assert len(equip.transform_equipment_failure_sensor(df_input).columns) == 1