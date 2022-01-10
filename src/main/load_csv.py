from pyspark.sql import SparkSession
from pathlib import Path

dir_path = Path(__file__).parent.parent

spark = SparkSession.Builder().getOrCreate()
df = spark.createDataFrame([("1","VP"), ("2", "AP")], ["id", "name"])
df2 = spark.read.option("header", "true").csv(f"{dir_path}/resources/data.csv")
df2.show()
df.show()