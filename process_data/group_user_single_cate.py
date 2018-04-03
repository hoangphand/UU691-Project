from __future__ import print_function
from __future__ import division
import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import random
import copy
import math

spark = SparkSession.builder.appName("basic-analytics").getOrCreate()
data = spark.read.csv("dataset/train.csv", header = "true")

# WRITE
# def map_products_by_categories(product):
# 	arr = []
# 	if product['Product_Category_1'] != None:
# 		arr.append(((product['User_ID'], int(product['Product_Category_1'])), 1))
	
# 	if product['Product_Category_2'] != None:
# 		arr.append(((product['User_ID'], int(product['Product_Category_2'])), 1))

# 	if product['Product_Category_3'] != None:
# 		arr.append(((product['User_ID'], int(product['Product_Category_3'])), 1))

# 	return arr

# products_with_separate_category = data.rdd.flatMap(map_products_by_categories)
# user_cate_purchased_count = products_with_separate_category.reduceByKey(lambda a, b: a + b)
# user_cate_purchased_count = user_cate_purchased_count.map(lambda x: (x[0][0], [(x[0][1], x[1])]))
# user_purchased_products_by_cate_rdd = user_cate_purchased_count.reduceByKey(lambda a, b: a + b)
# print(user_purchased_products_by_cate_rdd.collect())

# cate_dict = {}

# for index in range(1, 21):
# 	cate_dict[index] = 0

# def map_user_cate_to_dict(user):
# 	result = dict(cate_dict)

# 	for cate in user[1]:
# 		result[cate[0]] = cate[1]

# 	return (user[0], result)

# user_purchased_products_by_cate_dict = user_purchased_products_by_cate_rdd.map(map_user_cate_to_dict).collectAsMap()
# print(user_purchased_products_by_cate_dict)

# file = open("tmp_output_file/group_user_single_cate.ouput", 'w')
# for user in user_purchased_products_by_cate_dict:
# 	file.write(str(user))
# 	file.write(';')

# 	keys = user_purchased_products_by_cate_dict[user].keys()

# 	for index in range(0, len(keys)):
# 		file.write(str(keys[index]) + ':' + str(user_purchased_products_by_cate_dict[user][keys[index]]))
# 		if index != len(user_purchased_products_by_cate_dict[user]) - 1:
# 			file.write(',')
# 	file.write('\n')
# file.close()

# READER
User_Cate = Row('User_ID', 'Category')
def map_group_by_category_file(row):
	user_id = row.value.split(';')[0]
	result = {}
	cates = row.value.split(';')[1].split(',')
	for index in range(0, len(cates)):
		result[int(cates[index].split(':')[0])] = int(cates[index].split(':')[1])

	return (user_id, result)

group_user_single_cate_file = spark.read.text("tmp_output_file/group_user_single_cate.ouput").rdd
group_user_single_cate = group_user_single_cate_file.map(map_group_by_category_file)
group_user_single_cate_dict = group_user_single_cate.collectAsMap()
print(group_user_single_cate_dict)