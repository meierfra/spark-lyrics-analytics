import pyspark
import re
from pyspark.sql import SQLContext

sc = pyspark.SparkContext(appName="READ_DATA_TEST")

stopfile = "./stopwords.txt"
stopwords = set(sc.textFile(stopfile).collect())
# print(stopwords)


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


def combine_word_count_lists(wc1, wc2):
    d1 = dict(wc1)
    d2 = dict(wc2)
    for (k, v2) in d2.items():
        v1 = d1.get(k) or 0
        d1[k] = v1 + v2
    return d1.items()


def sort_word_count_list(word_count_list):
    return sorted(word_count_list, key=lambda x: x[1], reverse=True)


def count_unique_words(words):
    d = {}
    for word in words:
        n = d.get(word) or 0
        d[word] = n + 1
    return d.items()


def preproc_text(text):
    text = remove_word_binder(text)
    tokens = tokenize(text)
    tokens = remove_stop_words(tokens)
    word_counts = count_unique_words(tokens)
    word_counts = sort_word_count_list(word_counts)
    return word_counts


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


lyrics_rdd = lyrics_df.rdd
song_words_rdd = lyrics_rdd.map(lambda x: (x[0], x[2], preproc_text(x[1])))
# print(song_words_rdd.take(10))

print("----List most common words by song-------------")
for song in song_words_rdd.collect():
    print(song[0] + " | " + song[1] + " | " + str(len(song[2])) + " | " + str(song[2][0:3]))
print("")

print("----List most common words by artist-------------")
artist_words_rdd = song_words_rdd.map(lambda x: (x[0], x[2])).reduceByKey(lambda wc1, wc2: sort_word_count_list(combine_word_count_lists(wc1, wc2)))
for artist in artist_words_rdd.collect():
    print(artist[0] + " | " + str(artist[1][0:3]))
print("")

print("----List most common words in all songs-------------")
words_rdd = artist_words_rdd.map(lambda x: x[1]).reduce(lambda wc1, wc2: sort_word_count_list(combine_word_count_lists(wc1, wc2)))
print(words_rdd[0:3])
print("")
