import pyspark

from pyspark.shell import sqlContext

#sc = pyspark.SparkContext(appName="READ_DATA_TEST")
#dataRDD = sc.parallelize([1, 2, 3, 4, 5, 6], 2)
#print(dataRDD.reduce(lambda a, b: a + b))

lyrics_df = sqlContext.read.format("com.databricks.spark.csv")\
    .option("header", "true")\
    .option("multiLine", "true")\
    .option("escape", '"')\
    .option("inferSchema", "true")\
    .load("Lyrics.small.csv")
lyrics_df.printSchema()

print(lyrics_df.count())
lyrics_df.show()

