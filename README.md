# Answer to Shape's Data Engineering Interview


head -n 30 challenge-de-interview/equpment_failure_sensors.txt



>>> df = spark.read.text("s3a://equipment/delta/sample-equipment_failure_sensors.txt")
23/12/13 16:42:53 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties,hadoop-metrics2.properties
>>> df.show()
+--------------------+                                                          
|               value|
+--------------------+
|[2021-05-18 0:20:...|
|[2021-05-18 0:20:...|
|[2021-06-14 19:46...|
|[2020-09-27 22:55...|
|[2019-02-9 20:56:...|
|[2019-02-6 6:19:3...|
|[2019-08-10 20:23...|
|[2021-03-25 14:39...|
|[2020-05-15 17:30...|
|[2020-12-11 11:52...|
|[2019-04-16 8:28:...|
|[2020-10-1 20:8:3...|
|[2019-03-13 4:13:...|
|[2020-01-11 11:43...|
|[2020-02-9 13:57:...|
|[2020-04-22 18:30...|
|[2021-03-27 19:56...|
|[2020-06-3 9:37:1...|
|[2019-08-22 5:1:4...|
|[2021-02-10 6:0:1...|
+--------------------+