from pyspark import SparkContext

sc = SparkContext(appName="WordCountTest")
lines = sc.textFile("/home/ov-math/Desktop/text_example.txt")
words = lines.flatMap(lambda x: x.split(" "))
word_count = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y)
output = word_count.collect()

for pair in output:
	print(pair)