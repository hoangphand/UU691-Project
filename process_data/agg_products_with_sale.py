import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
import random
import operator

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

# WRITE
# products_with_sale = training.rdd.map(lambda row: (row['Product_ID'], 1)).reduceByKey(lambda a, b: a + b)

# file = open("tmp_output_file/products_with_sale.output", "w")
# for product in products_with_sale.collect():
# 	file.write(str(product[0]) + ":" + str(product[1]))
# 	file.write('\n')
# file.close()

# READER
def map_products_with_sale_file(line):
	return (line.value.split(':')[0], line.value.split(':')[1])

products_with_sale_file = spark.read.text("tmp_output_file/products_with_sale.output").rdd
products_with_sale = products_with_sale_file.map(map_products_with_sale_file)
products_with_sale_dict = products_with_sale.collectAsMap()
print(products_with_sale_dict)