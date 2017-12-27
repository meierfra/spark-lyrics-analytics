import re

stopfile = "./stopwords.txt"
stopwords = set()
with open(stopfile) as f:
    stopword_list = []
    for line in f:
        line = line.strip()
        if (len(line) > 0):
            stopword_list.append(line)
    stopwords = set(stopword_list)
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


def sort_word_count_list(word_count_list):
    return sorted(word_count_list, key=lambda x: x[1], reverse=True)


def count_unique_words(words):
    d = {}
    for word in words:
        n = d.get(word) or 0
        d[word] = n + 1
    return d.items()


# words = tokenize("hello world how up are you")
# print(words)
# words2 = remove_stop_words(words)
# print(words2)

print(sort_word_count_list(count_unique_words(tokenize("hello world hello world world"))))
