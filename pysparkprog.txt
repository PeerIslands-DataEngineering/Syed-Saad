Problem 1: Word Count (Excluding Stopwords)
Input: List of sentences

Task: Count how many times each word appears (except common words like "is", "the")

Steps:

Split sentences into words

Remove stopwords

Map words to (word, 1)

Use reduceByKey to count

Output: Word → Count

from pyspark import SparkContext

sc = SparkContext("local", "WordCountFiltering")

sentences = [
    "This is a simple sentence",
    "The word count program is written in PySpark",
    "PySpark is a powerful tool",
    "This tool is used for big data processing"
]

rdd = sc.parallelize(sentences)

stopwords = {"is", "the", "a", "an", "in", "for", "this"}

word_counts = (
    rdd.flatMap(lambda line: line.lower().split())
       .filter(lambda word: word not in stopwords)
       .map(lambda word: (word, 1))
       .reduceByKey(lambda x, y: x + y)
)

result = word_counts.collect()

for word, count in result:
    print(f"{word}: {count}")


 Problem 2: Average Score per Student
Input: List of (student_name, score)

Task: Calculate each student's average score

Steps:

Map to (name, (score, 1))

Use reduceByKey to sum scores and counts

Divide total_score by count

Output: Student → Average Score



from pyspark import SparkContext

sc = SparkContext.getOrCreate()

data = [("Alice", 80), ("Bob", 90), ("Alice", 70), ("Bob", 85), ("Charlie", 60)]

rdd = sc.parallelize(data)

avg_scores = (
    rdd.map(lambda x: (x[0], (x[1], 1)))
       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
       .map(lambda x: (x[0], x[1][0] / x[1][1]))
)

print(avg_scores.collect())



Problem 3: Number Frequency (Top 3)
Input: List of numbers

Task: Find how often each number appears, sort by frequency

Steps:

Map each number to (number, 1)

Use reduceByKey to count

Swap to (count, number)

Sort by count in descending order

Take top 3

Output: Top 3 frequent numbers

numbers = [5, 3, 4, 5, 2, 3, 5, 3, 4]

rdd = sc.parallelize(numbers)

freq_sorted = (
    rdd.map(lambda x: (x, 1))
       .reduceByKey(lambda a, b: a + b)
       .map(lambda x: (x[1], x[0]))
       .sortByKey(ascending=False)
)

print(freq_sorted.take(3))
