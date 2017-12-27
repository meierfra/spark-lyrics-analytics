import pyspark
import re
from pyspark.sql import SQLContext

sc = pyspark.SparkContext(appName="READ_DATA_TEST")
# dataRDD = sc.parallelize([1, 2, 3, 4, 5, 6], 2)
# print(dataRDD.reduce(lambda a, b: a + b))

stopfile = "./stopwords.txt"
stopwords = set(sc.textFile(stopfile).collect())
print(stopwords)


def remove_word_binder(text):
    remove_chars = r'[\'-]'
    return re.sub(remove_chars, '', text)


def tokenize(text):
    split_regex = r'\W+'
    l1 = re.split(split_regex, text)
    l2 = map(lambda word: word.lower(), l1)
    return filter(lambda word: len(word) > 0, l2)


def remove_stop_words(words):
    return filter(lambda word: word not in stopwords, words)


def preproc_text(text):
    return remove_stop_words(tokenize(remove_word_binder(text)))


sqlContext = SQLContext(sc)

lyrics_df = sqlContext.read.format("com.databricks.spark.csv")\
    .option("header", "true")\
    .option("multiLine", "true")\
    .option("escape", '"')\
    .option("inferSchema", "true")\
    .load("Lyrics.small.csv")

# lyrics_df.registerTempTable("lyrics")
# sqlContext.sql("select Band from lyrics").show()

# lyrics_df.printSchema()

# print(lyrics_df.count())
# lyrics_df.show()

test = lyrics_df.rdd.map(lambda x: (x[0], x[2], preproc_text(x[1]))).take(10)
print(test)
