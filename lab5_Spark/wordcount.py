import pyspark
from pyspark.sql import SparkSession

# Initialisation avec YARN
spark = SparkSession.builder.master("yarn").appName('wordcount').getOrCreate()

# Lecture du fichier depuis HDFS
data = spark.sparkContext.textFile("hdfs://hadoop-master:9000/user/root/input/alice.txt")

# Logique WordCount
words = data.flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Sauvegarde du résultat dans HDFS
wordCounts.saveAsTextFile("hdfs://hadoop-master:9000/user/root/output/rr2")

spark.stop()