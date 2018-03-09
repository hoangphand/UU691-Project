import sys
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import grouping
from pyspark.sql.functions import mean
from pyspark.sql.functions import udf

def map_products_by_categories(product):
	arr = []
	if product['Product_Category_1'] != None:
		arr.append((product['User_ID'], product['Product_ID'], int(product['Product_Category_1']), int(product['Purchase'])))
	
	if product['Product_Category_2'] != None:
		arr.append((product['User_ID'], product['Product_ID'], int(product['Product_Category_2']), int(product['Purchase'])))

	if product['Product_Category_3'] != None:
		arr.append((product['User_ID'], product['Product_ID'], int(product['Product_Category_3']), int(product['Purchase'])))

	return arr

def map_products_by_genders(product):
	arr = []

	pass

spark = SparkSession.builder.appName("basic-analytics").getOrCreate()
data = spark.read.csv("dataset/train.csv", header = "true").rdd

products_with_separate_category = data.flatMap(map_products_by_categories)
# print(products_with_separate_category.collect())

# distinct_categories = products_with_separate_category.map(lambda product: product[2]).distinct().collect()
# print(sorted(distinct_categories))

# BY CATEGORY
# count_product_sale_by_category = products_with_separate_category.map(lambda product: (product[2], product[3])).reduceByKey(lambda a, b: a + b).collect()
# print(sorted(count_product_sale_by_category))

# count_products_by_category = products_with_separate_category.map(lambda product: (product[2], 1)).groupByKey().mapValues(len).collect()
# print(sorted(count_products_by_category))

# BY GENDER
# count_products_by_gender = data.map(lambda product: (product['Gender'], 1)).groupByKey().mapValues(len).collect()
# print(count_products_by_gender)

# count_purchase_by_gender = data.map(lambda product: (product['Gender'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(count_purchase_by_gender)

# BY AGE
# count_product_sale_by_age = data.map(lambda product: (product['Age'], int(product['Purchase']))).reduceByKey(lambda a, b: a + b).collect()
# print(count_product_sale_by_age)

# count_products_by_age = data.map(lambda product: (product['Age'], 1)).reduceByKey(lambda a, b: a + b).collect()
# print(count_products_by_age)

# BY OCCUPATION
count_product_sale_by_occupation = data.map(lambda product: (product['Occupation'], product['Purchase'])).reduceByKey(lambda a, b: a + b).collect()
print(count_product_sale_by_occupation)
count_products_by_occupation = data.map(lambda product: (product['Occupation'], 1).reduceByKey(lambda a, b: a + b).collect()
print(count_products_by_occupation)

# print(products_with_separate_category.count())

# (training, test) = ratings.randomSplit([0.8, 0.2], seed=1234)

# preprocess data
# first approach: convert groups of age to 

# data.show()