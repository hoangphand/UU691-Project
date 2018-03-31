from __future__ import print_function
from __future__ import division
import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import random
import copy
import math

def map_collect_user_profile(user):
	return (user['User_ID'], user['Gender'], user['Age'], user['Occupation'], \
		user['City_Category'], user['Stay_In_Current_City_Years'], user['Marital_Status'])

spark = SparkSession.builder.appName("basic-analytics").getOrCreate()
data = spark.read.csv("dataset/train.csv", header = "true")

User = Row('User_ID', 'Gender', 'Age', 'Occupation', 'City_Category', 'Stay_In_Current_City_Years', 'Marital_Status')

distinct_users = data.rdd.map(map_collect_user_profile).map(lambda user: User(*user)).distinct()

print(distinct_users.collect())

def map_products_by_categories(product):
	arr = []
	if product['Product_Category_1'] != None:
		arr.append((product['Product_ID'], int(product['Product_Category_1'])))
	
	if product['Product_Category_2'] != None:
		arr.append((product['Product_ID'], int(product['Product_Category_2'])))

	if product['Product_Category_3'] != None:
		arr.append((product['Product_ID'], int(product['Product_Category_3'])))

	return arr

Product = Row('Product_ID', 'Product_Category')

training = training.rdd.flatMap(map_products_by_categories).map(lambda product: Product(*product)).distinct()