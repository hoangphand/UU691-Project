from __future__ import print_function
from __future__ import division
import sys
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import grouping
from pyspark.sql.functions import mean
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("basic-analytics").getOrCreate()

# WRITE
# READ TRAIN SET
# training = spark.read.csv("dataset/train.csv", header = "true")
# training_user_distinct = training.select('User_ID').distinct().rdd.map(lambda row: row['User_ID']).collect()
# training_product_distinct = training.select('Product_ID').distinct().rdd.map(lambda row: row['Product_ID']).collect()

# # READ TEST SET
# test = spark.read.csv("dataset/test.csv", header = "true")
# test_user_distinct = test.select('User_ID').distinct().rdd.map(lambda row: row['User_ID']).collect()
# test_product_distinct = test.select('Product_ID').distinct().rdd.map(lambda row: row['Product_ID']).collect()

# users_in_test_not_in_train = list(set(test_user_distinct) - set(training_user_distinct))
# products_in_test_not_in_train = list(set(test_product_distinct) - set(training_product_distinct))

# print("users in test set: " + str(len(test_user_distinct)))
# print("products in test set: " + str(len(test_product_distinct)))
# print("users: " + str(len(users_in_test_not_in_train)))
# print("products: " + str(len(products_in_test_not_in_train)))

# file = open("tmp_output_file/products_in_test_not_in_train.output", "w")
# for product in products_in_test_not_in_train:
# 	file.write(product + "\n")
# file.close()
# print(products_in_test_not_in_train)

# READER
products_in_test_not_in_train_file = spark.read.text("tmp_output_file/products_in_test_not_in_train.output").rdd
products_in_test_not_in_train = products_in_test_not_in_train_file.map(lambda line: line.value).collect()
print(products_in_test_not_in_train)