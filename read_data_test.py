import datetime
import re

import pyspark
from pyspark.sql import SQLContext

# pyspak API description
# https://spark.apache.org/docs/2.2.0/api/python/pyspark.html#pyspark.RDD
# https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame

PATH = "."
STOPWORD_FILE = PATH + "/" + "stopwords.txt"

# Lyrics dataset from kaggle
# https://www.kaggle.com/artimous/every-song-you-have-heard-almost
LYRICS_CSV = PATH + "/" + "Lyrics.small.csv"
# LYRICS_CSV = PATH + "/" + "Lyrics1.csv"
# LYRICS_CSV = PATH + "/" + "Lyrics2.csv"
# LYRICS_CSV = PATH + "/" + "Lyrics_all.csv"
SONGS_LIMIT = 10000
# SONGS_LIMIT = 10000000
PARTITIONS = 20

sc = pyspark.SparkContext(appName="READ_DATA_TEST")

stopwords = set(sc.textFile(STOPWORD_FILE).collect())
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


def combine_word_count_dicts(wcd1, wcd2):
    wcd = wcd1.copy()
    for (k, v2) in wcd2.items():
        v1 = wcd.get(k) or 0  # return 0 if word not in first word count dict
        wcd[k] = v1 + v2
    return wcd


def combine_word_count_lists(wc1, wc2):
    d1 = dict(wc1)
    d2 = dict(wc2)
    for (k, v2) in d2.items():
        v1 = d1.get(k) or 0
        d1[k] = v1 + v2
    return d1.items()


def sort_word_count_list(word_count_list):
    return sorted(word_count_list, key=lambda x: x[1], reverse=True)


def sort_word_count_dict_to_list(word_count_dict):
    # items gives a list in form [ (word1, count1), (word2, count2), ... ]
    return sorted(word_count_dict.items(), key=lambda x: x[1], reverse=True)


def count_unique_words_to_dict(words):
    d = {}
    for word in words:
        n = d.get(word) or 0  # if d.get(word) is None use 0
        d[word] = n + 1
    return d


def count_unique_words_to_list(words):
    return count_unique_words_to_dict(words).items()


def preproc_text(text):
    text = remove_word_binder(text)
    tokens = tokenize(text)
    tokens = remove_stop_words(tokens)
    word_count_dict = count_unique_words_to_dict(tokens)
    return word_count_dict


t0 = datetime.datetime.now()

sqlContext = SQLContext(sc)
lyrics_df = sqlContext.read.format("com.databricks.spark.csv")\
    .option("header", "true")\
    .option("multiLine", "true")\
    .option("escape", '"')\
    .option("inferSchema", "true")\
    .load(LYRICS_CSV)
print("loaded {} lyrics into data frame".format(lyrics_df.count()))
t_loaded = datetime.datetime.now()
print("loadtime:", str(t_loaded - t0))

# lyrics_df.registerTempTable("lyrics")
# sqlContext.sql("select Band from lyrics").show()

# lyrics_df.printSchema()

# print(lyrics_df.count())
# lyrics_df.show()

# Get SONGS_LIMIT Rows/Songs out of lyrics_df
# repartition() is needed for performance
lyrics_rdd_raw = lyrics_df.limit(SONGS_LIMIT).rdd.repartition(PARTITIONS)
# reorder structture from |Artist|Lyrics|Songname| to |Artist|Songname|Lyrics|
lyrics_rdd = lyrics_rdd_raw.map(lambda x: (x[0], x[2], x[1])).cache()

# preprocess the lyrics
song_words_rdd = lyrics_rdd.map(lambda x: (x[0], x[1], preproc_text(x[2]))).cache()
print("song_words_rdd with {} entries and {} partitions".format(song_words_rdd.count(), song_words_rdd.getNumPartitions()))
# print(song_words_rdd.take(10))
t_preproc = datetime.datetime.now()
print("preprocesstime:", str(t_preproc - t_loaded))
print("...\n")

print("----List most common words by song-------------")
for song in song_words_rdd.map(lambda x: (x[0], x[1], sort_word_count_dict_to_list(x[2]))).take(20):
    # print Artist|SongName|Number of unique words in Song|3 most used words|
    print(song[0] + " | " + song[1] + " | " + str(len(song[2])) + " | " + str(song[2][0:3]))
print("...")
print("")

print("----List most common words by artist-------------")
# create new rrd with structure |Interpret|word count dict| then reduce by key (Interpret), combining the word count dictionaries
artist_words_rdd = song_words_rdd.map(lambda x: (x[0], x[2])).reduceByKey(lambda wcd1, wcd2: combine_word_count_dicts(wcd1, wcd2)).cache()
# print("artist_words_rdd partitions", artist_words_rdd.getNumPartitions())
for artist in artist_words_rdd.map(lambda x: (x[0], sort_word_count_dict_to_list(x[1]))).take(10):
    print(artist[0] + " | " + str(artist[1][0:3]))
print("...\n")


print("----List most common words in all songs-------------")
words_count_dict = song_words_rdd.map(lambda x: x[2]).reduce(lambda wcd1, wcd2: combine_word_count_dicts(wcd1, wcd2))
print(sort_word_count_dict_to_list(words_count_dict)[0:10])
print("")


print("----List artists using the most unique words-------------")
# reuse artist_words_rdd and sort by length of the dictionary which is the number of unique words
artist_words_count_sorted_rdd = artist_words_rdd.map(lambda x: (x[0], len(x[1]))).sortBy(lambda x: x[1], ascending=False).cache()

for artist_word_count in artist_words_count_sorted_rdd.take(10):
    print(artist_word_count[0] + " | " + str(artist_word_count[1]))
print("...\n")


print("----List artists with the longest texts by average-------------")
# to calculate the average we first create a tuple with length of the lyrics and the literal "1" for each song: (len(lyrics) , 1).
# Then we reduce and sum the tuple resolving the total length and the number of songs of the artist: (len(all lyrics, num lyrics)).
# Then we can map the rdd again and divide the total length by the number resulting in the average.
artist_lyrics_len_avg_rdd = lyrics_rdd.map(lambda x: (x[0], (len(x[2]), 1)))\
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
    .map(lambda x: (x[0], x[1][0] / x[1][1]))\
    .sortBy(lambda x: x[1], ascending=False)

for artist_lyrics_len in artist_lyrics_len_avg_rdd.take(10):
    print(artist_lyrics_len[0] + " | " + str(artist_lyrics_len[1]))
print("...\n")


t_end = datetime.datetime.now()
print("runtime:", str(t_end - t_preproc))
