# $example on$
from pyspark.sql import SparkSession

spark = SparkSession.build \
  .master("local") \
  .appName("Word Count") \
  .config("spark.some.config.option", "some-value") \
  .getOrCreate()
# $example off$
