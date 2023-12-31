from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import Column, col, current_timestamp, expr, lit, split, regexp_replace, to_timestamp, count, avg, when, to_date, coalesce, year
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from delta.tables import DeltaTable


@dataclass
class DataSetConfig:
    name: str
    curr_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    database: str
    partition: str
    skip_publish: bool = False
    replace_partition: bool = False

class StandardETL(ABC):

    def __init__(
        self,
        storage_path: Optional[str] = None,
        database: Optional[str] = None,
        partition: Optional[str] = None,
    ):
        self.STORAGE_PATH = storage_path or 's3a://equipment/delta'
        self.DATABASE = database or 'equipment'
        self.DEFAULT_PARTITION = partition or datetime.now().strftime(
            "%Y-%m-%d"
        )

    @abstractmethod
    def get_bronze_datasets(self, spark: SparkSession, **kwargs) -> Dict[str, DataFrame]:
        pass
    
    @abstractmethod
    def get_silver_datasets(self, spark: SparkSession, **kwargs) -> Dict[str, DataFrame]:
        pass
    
    @abstractmethod
    def get_gold_datasets(self, spark: SparkSession, **kwargs) -> Dict[str, DataFrame]:
        pass
    
    def construct_join_string(self, keys: List[str]) -> str:
        return ' AND '.join([f"target.{key} = source.{key}" for key in keys])

    def publish_data(
        self,
        input_datasets: Dict[str, DataSetConfig],
        spark: SparkSession,
        **kwargs,
    ) -> None:
        for input_dataset in input_datasets.values():
            if not input_dataset.skip_publish:
                curr_data = input_dataset.curr_data.withColumn(
                    'etl_inserted', current_timestamp()
                ).withColumn('partition', lit(input_dataset.partition))
                if input_dataset.replace_partition:
                    curr_data.write.format("delta").mode("overwrite").option(
                        "replaceWhere",
                        f"partition = '{input_dataset.partition}'",
                    ).save(input_dataset.storage_path)
                else:
                    targetDF = DeltaTable.forPath(
                        spark, input_dataset.storage_path
                    )
                    (
                        targetDF.alias("target")
                        .merge(
                            curr_data.alias("source"),
                            self.construct_join_string(
                                input_dataset.primary_keys
                            ),
                        )
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )

class EquipmentETL(StandardETL):
    
    def get_dim_equipment(
        self,
        equipment: DataSetConfig, 
        **kwargs
    ) -> DataFrame:
        
        equipment_df = equipment.curr_data
        dim_equipment = kwargs["dim_equipment"]
        equipment_df = equipment_df.withColumn(
            "equipment_sk",
            expr("md5(concat(equipment_id, updated_at_dt))")
        )
        dim_equipment_latest = dim_equipment.where("current = True")
        

        equipment_df_insert_net_new = (
            equipment_df.join(
                dim_equipment_latest,
                (equipment_df.equipment_id == dim_equipment_latest.equipment_id)
                & (
                    dim_equipment_latest.updated_at_dt < equipment_df.updated_at_dt
                ),
                "leftanti",
            ).select(
                equipment_df.equipment_sk,
                equipment_df.equipment_id,
                equipment_df.name,
                equipment_df.group_name,
                equipment_df.updated_at_dt
            )
            .withColumn('current', lit(True))
            .withColumn('valid_from', equipment_df.updated_at_dt)
            .withColumn('valid_to', lit('2099-01-01 12:00:00.0000'))
        )

        equipment_df_insert_existing_ids = (
            equipment_df.join(
                dim_equipment_latest,
                (equipment_df.equipment_id == dim_equipment_latest.equipment_id)
                & (
                    dim_equipment_latest.updated_at_dt
                    < equipment_df.updated_at_dt
                ),
            )
            .select(
                equipment_df.equipment_sk,
                equipment_df.equipment_id,
                equipment_df.name,
                equipment_df.group_name,
                equipment_df.updated_at_dt
            )
            .withColumn('current', lit(True))
            .withColumn('valid_from', equipment_df.updated_at_dt)
            .withColumn('valid_to', lit('2099-01-01 12:00:00.0000'))
        )

        equipment_df_update = (
            equipment_df.join(
                dim_equipment_latest,
                (equipment_df.equipment_id == dim_equipment_latest.equipment_id)
                & (
                    dim_equipment_latest.updated_at_dt
                    < equipment_df.updated_at_dt
                )
            )
            .select(
                dim_equipment.equipment_sk,
                dim_equipment.equipment_id,
                dim_equipment.name,
                dim_equipment.group_name,
                equipment_df.updated_at_dt,
                dim_equipment.valid_from
            )
            .withColumn('current', lit(False))
            .withColumn('valid_to', equipment_df.updated_at_dt)
        )

        return equipment_df_insert_net_new.unionByName(
            equipment_df_insert_existing_ids
        ).unionByName(
            equipment_df_update
        )
    
    def _get_equipment(self, spark: SparkSession) -> DataFrame:
        return spark.read.json(f"{self.STORAGE_PATH}/data/equipment/equipment/", multiLine=True).withColumn("updated_at_dt", expr("current_timestamp")).withColumn("equipment_id", col("equipment_id").cast("string"))

    def _get_equipment_sensors(self, spark: SparkSession) -> DataFrame:
        return spark.read.csv(f"{self.STORAGE_PATH}/data/equipment/equipment_sensors/", header=True)
    
    def _get_equipment_failure_sensors(self, spark) -> DataFrame:
        return spark.read.text(f"{self.STORAGE_PATH}/data/equipment/equipment_failure_sensors/")

    def transform_equipment_failure_sensor(self, equipment_failures: DataFrame) -> DataFrame:
        def to_timestamp_(col, formats=("MM/dd/yyyy", "yyyy-MM-dd")):
            return coalesce(*[to_timestamp(col, f) for f in formats])
        
        treat_err_value = lambda column: when(column == "err", lit(0)).otherwise(column)

        df_equipment_failures = equipment_failures
        df_equipment_failures = df_equipment_failures.select(
            split("value", "\t").getItem(0).alias("created_at_dt"),
            split("value", "\t").getItem(1).alias("log_level"),
            split("value", "\t").getItem(2).alias("sensor_id"),
            split("value", "\t").getItem(4).alias("temperature"),
            split("value", "\t").getItem(5).alias("vibration")
        )
        df_equipment_failures = df_equipment_failures.withColumn(
            "created_at_dt",
            regexp_replace("created_at_dt", "(\[|\])", "")
        ).withColumn(
            "created_at_dt",
            to_timestamp_(regexp_replace("created_at_dt", "\/", "-"), formats=["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"])
        )
        
        df_equipment_failures = df_equipment_failures.withColumn(
            "sensor_id",
            regexp_replace("sensor_id","\D", "")
        ).withColumn(
            "temperature",
            treat_err_value(regexp_replace("temperature", "vibration|\,", "")).cast("decimal(18,2)")
        ).withColumn(
            "vibration",
            treat_err_value(regexp_replace("vibration", "\)", "")).cast("decimal(18,2)")
        )


        return df_equipment_failures

    def get_bronze_datasets(self, spark: SparkSession, **kwargs) -> Dict[str, DataSetConfig]:
        return {
            "equipment": DataSetConfig(
                name="equipment",
                curr_data=self._get_equipment(spark),
                primary_keys=["equipment_id"],
                storage_path=f"{self.STORAGE_PATH}/bronze/equipment/equipment",
                table_name="equipment",
                database=self.DATABASE,
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True
            ),
            "equipment_sensors": DataSetConfig(
                name="equipment_sensors",
                curr_data=self._get_equipment_sensors(spark),
                primary_keys=["equipment_id", "sensor_id"],
                storage_path=f"{self.STORAGE_PATH}/bronze/equipment/equipment_sensors",
                table_name="equipment_sensors",
                database=self.DATABASE,
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True
            ),
            "equipment_failure_sensors": DataSetConfig(
                name="equipment_sensors",
                curr_data=self._get_equipment_failure_sensors(spark),
                primary_keys=[],
                storage_path=f"{self.STORAGE_PATH}/bronze/equipment/equipment_failure_sensors",
                table_name="equipment_failure_sensors",
                database=self.DATABASE,
                partition=kwargs.get("partition", self.DEFAULT_PARTITION),
                replace_partition=True
            )
        }

    def get_silver_datasets(self, spark: SparkSession, input_datasets: Dict[str, DataSetConfig], **kwargs) -> Dict[str, DataSetConfig]:

        dim_equipment_df = self.get_dim_equipment(
            input_datasets["equipment"],
            dim_equipment=spark.read.table(f"{self.DATABASE}.dim_equipment")
        )
        silver_datasets = {}
        
        silver_datasets["dim_equipment"] = DataSetConfig(
            name="dim_equipment",
            curr_data=dim_equipment_df,
            primary_keys=["equipment_id"],
            storage_path=f"{self.STORAGE_PATH}/silver/equipment/dim_equipment/",
            table_name="dim_equipment",
            database=self.DATABASE,
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
        )
        
        self.publish_data(silver_datasets, spark)
        silver_datasets['dim_equipment'].skip_publish = True
        
        silver_datasets["equipment_failure_sensors"] = DataSetConfig(
            name="equipment_failure_sensors",
            curr_data=self.transform_equipment_failure_sensor(input_datasets["equipment_failure_sensors"].curr_data),
            primary_keys=["equipment_id"],
            storage_path=f"{self.STORAGE_PATH}/silver/equipment/equipment_failure_sensors/",
            table_name="equipment_failure_sensors",
            database=self.DATABASE,
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
            replace_partition=True
        )
        return silver_datasets
    
    def get_failure_mart(self, spark, input_datasets: Dict[str, DataSetConfig], **kwargs) -> DataFrame:
        dim_equipment = input_datasets["dim_equipment"].curr_data.where("current = true")
        equipment_sensors = spark.read.table(f"{self.DATABASE}.equipment_sensors")
        equipment_failure_sensor = input_datasets["equipment_failure_sensors"].curr_data

        equipment_failure_sensor = equipment_failure_sensor.filter(year("created_at_dt").isin([2019]))
        equipment_failure_sensor =  equipment_failure_sensor.join(
            equipment_sensors,
            equipment_sensors.sensor_id == equipment_failure_sensor.sensor_id,
            "left"
        ).select(
            equipment_failure_sensor.log_level,
            equipment_failure_sensor.sensor_id,
            equipment_failure_sensor.temperature,
            equipment_failure_sensor.vibration,
            to_date(equipment_failure_sensor.created_at_dt).alias("created_at_dt"),
            equipment_sensors.equipment_id
        )
        
        equipment_failure_sensor=  equipment_failure_sensor.join(
            dim_equipment,
            equipment_failure_sensor.equipment_id == dim_equipment.equipment_id,
            "left"
        ).select(
            "*",
            dim_equipment.name.alias("equipment_name"),
            dim_equipment.group_name,
            dim_equipment.equipment_sk,
            dim_equipment.equipment_id
        )        
        equipment_failure_sensor = equipment_failure_sensor.groupBy(
            dim_equipment.equipment_id,
            "equipment_name",
            dim_equipment.group_name,
            "sensor_id",
            "log_level",
            "created_at_dt",
        ).agg(
            count("sensor_id").cast("int").alias("count"),
            avg("temperature").cast("decimal(18,2)").alias("avg_temperature"),
            avg("vibration").cast("decimal(18,2)").alias("avg_vibration")
        )

        return equipment_failure_sensor

    def get_gold_datasets(self, spark: SparkSession, input_datasets: Dict[str, DataSetConfig], **kwargs) -> Dict[str, DataSetConfig]:
    
        return {
           "equipment_failure_mart": DataSetConfig(
                name="equipment_failure_mart",
                curr_data=equipment_failure_sensor,
                primary_keys=[""],
                storage_path=f"s3a://equipment/delta/gold/equipment/equipment_failure_mart/",
                table_name="equipment_failure_mart",
                database="equipment",
                partition="2023-12-16",
                replace_partition=True
            )
        }
    
    def run(self, spark: SparkSession) -> None:

        bronze_datasets = self.get_bronze_datasets(spark)
        self.publish_data(bronze_datasets, spark)
        silver_datasets = self.get_silver_datasets(spark, bronze_datasets)
        self.publish_data(silver_datasets, spark)
        gold_datasets = self.get_gold_datasets(spark, silver_datasets)
        self.publish_data(gold_datasets, spark)

if __name__ == "__main__":

    spark = SparkSession.builder.appName("equipment").enableHiveSupport().getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    equipment = EquipmentETL()
    equipment.run(spark)
    spark.stop()