from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
sc = SparkContext(appName="StreamingErrorCount")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)
#ssc.checkpoint("hdfs:///user/hdp/streaming")
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) 

counts = lines.flatMap(lambda line: line.split(" "))\
    .filter(lambda word:"ERROR" in word)\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a+b)
    
counts.pprint() 

ssc.start() 
ssc.awaitTermination()
