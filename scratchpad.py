import re

stopfile = "./stopwords.txt"
stopwords = set()
with open(stopfile) as f:
    stopword_list = []
    for line in f:
        line = line.strip()
        if (len(line) > 0):
            print(line)
            stopword_list.append(line)
    stopwords = set(stopword_list)
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


words = tokenize("hello world how up are you")
print(words)
words2 = remove_stop_words(words)
print(words2)
