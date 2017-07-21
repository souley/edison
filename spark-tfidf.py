#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 11 15:16:46 2017

@author: souley
"""
from pyspark.sql import SparkSession
#from pyspark.ml.feature import HashingTF, IDF, Tokenizer, RegexTokenizer
##import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
#import org.apache.spark.sql.functions._

from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("Spark CV-job ad matching") \
    .config("spark.some.config.option", "some-value") \
    .master("local[*]") \
    .getOrCreate()

#sentenceData = spark.createDataFrame([
#    (0.0, "Hi I heard about Spark"),
#    (0.0, "I wish Java could use case classes"),
#    (1.0, "Logistic regression models are neat")
#], ["label", "sentence"])
#
#tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
#wordsData = tokenizer.transform(sentenceData)
#
#hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
#featurizedData = hashingTF.transform(wordsData)
## alternatively, CountVectorizer can also be used to get term frequency vectors
#
#idf = IDF(inputCol="rawFeatures", outputCol="features")
#idfModel = idf.fit(featurizedData)
#rescaledData = idfModel.transform(featurizedData)
#
#rescaledData.select("label", "features").show()

sentenceDataFrame = spark.createDataFrame([
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic regression,models,are,neat")
], ["id", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W,")
# alternatively, pattern="\\w+", gaps(False)

countTokens = udf(lambda words: len(words), IntegerType())

#tokenized = tokenizer.transform(sentenceDataFrame)
#tokenized.select("sentence", "words")\
#    .withColumn("tokens", countTokens(col("words"))).show(truncate=False)

regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("sentence", "words") \
    .withColumn("tokens", countTokens(col("words"))).show(truncate=False)