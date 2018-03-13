import sys
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import grouping
from pyspark.sql.functions import mean
from pyspark.sql.functions import udf

import plotly

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
# count_product_sale_by_category = products_with_separate_category.map(lambda product: (product[2], product[3])).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_product_sale_by_category))
# count_products_by_category = products_with_separate_category.map(lambda product: (product[2], 1)).groupByKey().mapValues(len).collect()
# print(sorted(count_products_by_category))

# BY GENDER
# print("By Gender")
# count_products_by_gender = data.map(lambda product: (product['Gender'], 1)).groupByKey().mapValues(len).collect()
# print(sorted(count_products_by_gender))
# count_purchase_by_gender = data.map(lambda product: (product['Gender'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_purchase_by_gender))

# BY AGE
# print("By Age")
# count_product_sale_by_age = data.map(lambda product: (product['Age'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_product_sale_by_age))
# count_products_by_age = data.map(lambda product: (product['Age'], 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products_by_age))

# BY OCCUPATION
# print("By Occupation")
# count_product_sale_by_occupation = data.map(lambda product: (int(product['Occupation']), int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_product_sale_by_occupation))
# count_products_by_occupation = data.map(lambda product: (int(product['Occupation']), 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products_by_occupation))

# print("By Marital Status")
# count_product_sale_by_marital_status = data.map(lambda product: (int(product['Marital_Status']), int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_product_sale_by_marital_status))
# count_products_by_marital_status = data.map(lambda product: (int(product['Marital_Status']), 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products_by_marital_status))

# print("By City")
# count_product_sale_by_city = data.map(lambda product: (product['City_Category'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_product_sale_by_city))
# count_products_by_city = data.map(lambda product: (product['City_Category'], 1)).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_products_by_city))

print("By Length of Stay in the City")
count_product_sale_by_length_of_stay = data.map(lambda product: (product['Stay_In_Current_City_Years'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
print(sorted(count_product_sale_by_length_of_stay))
count_products_by_length_of_stay = data.map(lambda product: (product['Stay_In_Current_City_Years'], 1)).reduceByKey(lambda a, b: a + b).collect()
print(sorted(count_products_by_length_of_stay))

# print(products_with_separate_category.count())

# (training, test) = ratings.randomSplit([0.8, 0.2], seed=1234)

# preprocess data
# first approach: convert groups of age to 

# data.show()