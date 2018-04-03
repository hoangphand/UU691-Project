import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
import operator

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
test = spark.read.csv("dataset/test.csv", header = "true")
train = spark.read.csv("dataset/train.csv", header = "true")

distinct_products_train = train.rdd.map(lambda tx: tx['Product_ID']).distinct().collect()
distinct_products_test = test.rdd.map(lambda tx: tx['Product_ID']).distinct().collect()

new_products = set(distinct_products_test) - set(distinct_products_train)

file = open("tmp_output_file/new_products_in_test_set.output", "w")
for el in new_products:
	file.write(el + "\n")
file.close()