%spark.pyspark
# Modules needed
import datetime
import re
import pyspark
from pyspark.sql import SQLContext

# Create pyspark context
sqlContext = SQLContext(sc)

# Raw data path
filename1 = '/user/hadoop/Lyrics1.csv'
filename2 = '/user/hadoop/Lyrics2.csv'
# Stopwords path and create context
STOPWORD_FILE = '/user/hadoop/stopwords.txt'
stopwords = set(sc.textFile(STOPWORD_FILE).collect())

# Functions
def count_unique_words_to_dict(words):
    '''Create a dictionary with the unique tokens for each song lyrics
    Args:
        words (list)
    return:
        d (dict), dictionary of unique words'''
    d = {}
    for word in words:
        n = d.get(word) or 0  # if d.get(word) is None use 0
        d[word] = n + 1
    return d

def remove_stop_words(words):
    '''Remove the stopwords in accordance with the set of stopwords created
    Args:
        words (list), a list of tokens
    return:
        a list of tokens with the stopwords removed'''
    return filter(lambda word: word not in stopwords, words)

def tokenize(text):
    '''Split the song lyrics into single words (tokens)
    Args:
        text (string), the sond lyrics
    return:
        a list of tokens'''
    split_regex = r'\W+'
    l1 = re.split(split_regex, text)
    l2 = map(lambda word: word.lower(), l1)
    return filter(lambda word: len(word) > 0, l2)

def remove_word_binder(text):
    '''Remove unwanted characters parsed from the raw data using the sub method for regular expressions
    Args: 
        text (string), the song lyrics
    return:
        text (string), the modified song lyrics after parsing it with the regular expression defined '''
    remove_chars = r'[\'-]'
    return re.sub(remove_chars, '', text)

def preproc_text(text):
    '''Preprocess the data using the functions defined above '''
    text = remove_word_binder(text)
    tokens = tokenize(text)
    tokens = remove_stop_words(tokens)
    word_count_dict = count_unique_words_to_dict(tokens)
    return word_count_dict

# Create dataframes from the raw data
lyrics_df1 = sqlContext.read.format("csv")\
    .option("header", "true")\
    .option("multiLine", "true")\
    .option("escape", '"')\
    .option("inferSchema", "true")\
    .load(filename1)

lyrics_df2 = sqlContext.read.format("csv")\
    .option("header", "true")\
    .option("multiLine", "true")\
    .option("escape", '"')\
    .option("inferSchema", "true")\
    .load(filename2)
    
# Concatenate dataframes
lyrics_df_union = lyrics_df1.union(lyrics_df1)

# Remove duplicates (if any)
lyrics_df = lyrics_df_union.dropDuplicates()

# Limit songs and repartition
lyrics_rdd_raw = lyrics_df.limit(500000).rdd.repartition(20)

# Reorder structture from |Artist|Lyrics|Song| to |Artist|Song|Lyrics|
# and filter out elements with empty values
lyrics_rdd = lyrics_rdd_raw.map(lambda x: (x[0], x[2], x[1])).filter(lambda x: x[0] and x[1] and x[2]).cache()

# Preprocess the RDD
song_words_rdd = lyrics_rdd.map(lambda x: (x[0], x[1], preproc_text(x[2]))).cache()

# Get number of words per song
wordsPerSong_rdd = song_words_rdd.map(lambda x: (x[0], x[1], sum(x[2].values()))).cache()

# Create dataframe
schemaWordPerSong = sqlContext.createDataFrame(wordsPerSong_rdd, ['artist','song','words'])

# Register table to perform SQL queries
schemaWordPerSong.registerTempTable("lyrics")

%spark.pyspark
lyrics_df.count(), lyrics_rdd.count(), song_words_rdd.count()

%spark.sql
SELECT count(song)
FROM lyrics

%spark.sql
SELECT count(distinct(artist))
FROM lyrics

%spark.sql
SELECT song, words
FROM lyrics
ORDER BY words DESC
LIMIT 10

%spark.sql
SELECT *
FROM lyrics
WHERE song = 'Foxy Music' or
      words = 2284 or words = 1680

%spark.sql
SELECT artist, count(1) songs
FROM lyrics
GROUP BY artist
ORDER BY songs DESC
Limit 10

%spark.sql
SELECT *
FROM lyrics
WHERE artist = 'Frank Sinatra'
