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

# import plotly

def map_products_by_categories(product):
	arr = []
	if product['Product_Category_1'] != None:
		arr.append((product['User_ID'], product['Product_ID'], int(product['Product_Category_1']), int(product['Purchase'])))
	
	if product['Product_Category_2'] != None:
		arr.append((product['User_ID'], product['Product_ID'], int(product['Product_Category_2']), int(product['Purchase'])))

	if product['Product_Category_3'] != None:
		arr.append((product['User_ID'], product['Product_ID'], int(product['Product_Category_3']), int(product['Purchase'])))

	return arr

spark = SparkSession.builder.appName("basic-analytics").getOrCreate()
data = spark.read.csv("dataset/train.csv", header = "true").rdd

products_with_separate_category = data.flatMap(map_products_by_categories)
# print(products_with_separate_category.collect())

# distinct_categories = products_with_separate_category.map(lambda product: product[2]).distinct().collect()
# print(sorted(distinct_categories))

# BY CATEGORY
# print("By Category")
# count_product_sale_by_category = products_with_separate_category.map(lambda product: (product[2], product[3])).reduceByKey(lambda a, b: a + b)
# # print(sorted(count_product_sale_by_category))
# total_sale = 0
# for el in count_product_sale_by_category.collect():
# 	total_sale += el[1]
# # total_sale = count_product_sale_by_category.reduce(lambda a, b: a[1] + b[1])
# # print(total_sale)
# for cate in sorted(count_product_sale_by_category.collect()):
# 	print(str(cate[0]) + ": " + str(round(cate[1] / total_sale * 100, 2)))

# count_products_by_category = products_with_separate_category.map(lambda product: (product[2], 1)).groupByKey().mapValues(len).collect()
# total_items_sold = 0
# for el in count_products_by_category:
# 	total_items_sold += el[1]
# # print(sorted(count_products_by_category))
# for cate in sorted(count_products_by_category):
# 	print(str(cate[0]) + ": " + str(round(cate[1] / total_items_sold * 100, 2)))

# BY GENDER
# print("By Gender")
# count_products = data.map(lambda product: (product['Gender'], 1)).groupByKey().mapValues(len).collect()
# print(sorted(count_products))


# count_sale = data.map(lambda product: (product['Gender'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_sale))

# BY AGE
# print("By Age")
# count_sale = data.map(lambda product: (product['Age'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_sale))
# count_products = data.map(lambda product: (product['Age'], 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products))

# BY OCCUPATION
# print("By Occupation")
# count_sale = data.map(lambda product: (int(product['Occupation']), int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_sale))
# count_products = data.map(lambda product: (int(product['Occupation']), 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products))

# print("By Marital Status")
# count_sale = data.map(lambda product: (int(product['Marital_Status']), int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_sale))
# count_products = data.map(lambda product: (int(product['Marital_Status']), 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products))

# print("By City")
# count_sale = data.map(lambda product: (product['City_Category'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_sale))
# count_products = data.map(lambda product: (product['City_Category'], 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products))

print("By Length of Stay in the City")
count_sale = data.map(lambda product: (product['Stay_In_Current_City_Years'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
print(sorted(count_sale))
count_products = data.map(lambda product: (product['Stay_In_Current_City_Years'], 1)).reduceByKey(lambda a, b: a + b).collect()
print(sorted(count_products))

# print(products_with_separate_category.count())

# (training, test) = ratings.randomSplit([0.8, 0.2], seed=1234)

# preprocess data
# first approach: convert groups of age to 

# data.show()
total_items_sold = 0
for el in count_products:
	total_items_sold += el[1]
for cate in sorted(count_products):
	print(str(cate[0]) + ": " + str(round(cate[1] / total_items_sold * 100, 2)))
total_sale = 0
for el in count_sale:
	total_sale += el[1]
for cate in sorted(count_sale):
	print(str(cate[0]) + ": " + str(round(cate[1] / total_sale * 100, 2)))