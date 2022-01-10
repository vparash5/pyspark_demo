from pyspark.sql import SparkSession

spark = SparkSession.Builder().getOrCreate()
df = spark.createDataFrame([("1","VP"), ("2", "AP")], ["id", "name"])
df.show()