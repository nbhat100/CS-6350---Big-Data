import sys
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, desc, to_json, struct
from pyspark.sql.types import ArrayType, StringType
from kafka import KafkaProducer

#import spacy
if len(sys.argv) != 5:
    print("""
        Usage: consumeRedditData.py <bootstrap-servers> <subscribe-type> <inputTopic> <outputTopic> 
        """, file=sys.stderr)
    sys.exit(-1)

bootstrapServers = sys.argv[1]
subscribeType = sys.argv[2]
inputTopic = sys.argv[3]
outputTopic = sys.argv[4]

spark = SparkSession\
        .builder\
        .appName("RedditNER")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create DataSet representing the stream of input lines from kafka
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, inputTopic)\
        .load()\
        .selectExpr("CAST(value AS STRING)")

# print(lines.value)

nlp = spacy.load("en_core_web_sm")
def perform_ner(text):
    doc = nlp(text)
    # Extract named entities from the doc
    entities = [ent.text for ent in doc.ents]
    return entities
'''
# Apply NER to all lines of the book, and sort the words by decreasing order of word count
linesRDD = lines.rdd.map(tuple)
result = linesRDD.flatMap(lambda x: perform_ner(x)).map(lambda x: (x[0].lower(), 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: -x[1])
resultDataFrame = result.toDF()
'''
performNerUdf = spark.udf.register("perform_ner", perform_ner, ArrayType(StringType()))
resDF = lines.select(explode(performNerUdf(lines.value)).alias("nerEntity"))
resDF = resDF.select(resDF["nerEntity"].alias("Entity"))
resultDataFrame = resDF.groupBy("entity").count().orderBy(desc("count"))
#resultJson = resultDataFrame.selectExpr("to_json(struct(*)) as value")
# Printing the counts to the console for testing
#.select(to_json(struct("entity", "count")).alias("value"))\
def writeToKafka(df, idVal):
    # convert to the dataframe to json and send to the output topic
    df.select(to_json(struct("entity", "count")).alias("value"))\
        .write\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("topic", outputTopic)\
        .save()

wordCounts = resultDataFrame\
        .writeStream\
        .outputMode("complete")\
        .option("checkpointLocation", "/tmp")\
        .foreachBatch(writeToKafka) \
        .start()
        
wordCounts.awaitTermination()
