#  ETL Pipeline Pyspark

The objective of this task is to create an ETL job which will read data from a file, transform it into the desired state and save it to an output location.
For each charge point, identified by its unique ID (CPI), we would like to know the duration (in hours) of the longest plugin and the duration (in hours of the average plugin.
extract - this method should return a Spark dataframe which contains raw data from the input file in input_path.
transform - this method should get a raw dataframe as an input parameter and return a dataframe containing the following three columns:
chargepoint_id, max_duration, avg_duration
load - this method should take this transformed dataframe as input parameter and save the data as parquet format to output path in output_path.


Sample of the result obtained:

+--------------+------------+------------+
|chargepoint_id|max_duration|avg_duration|
+--------------+------------+------------+
|       AN03946|       13.35|        7.88|
|       AN00218|        3.46|        1.56|
|       AN08663|       41.71|       15.95|
|       AN05089|       12.85|        9.78|
|       AN08083|        2.73|        1.46|
|       AN00603|       62.92|       28.72|
|       AN04630|       34.32|       22.94|
|       AN16172|       14.93|       11.24|
|       AN06965|        25.4|        8.99|
|       AN08377|       23.95|       17.82|
|       AN10884|        24.1|        16.9|
|       AN11526|       17.87|       11.58|
|       AN09764|       14.58|        5.92|
|       AN06739|       11.79|         4.9|
|       AN12429|       22.57|        13.1|
|       AN03896|       13.09|        5.41|
|       AN17688|       14.99|        6.71|
|       AN09713|       10.22|        4.07|
|       AN15995|        3.78|        3.56|
|       AN11463|        12.6|        7.35|
+--------------+------------+------------+
only showing top 20 rows
