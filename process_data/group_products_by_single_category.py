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
# 		arr.append(((int(product['Product_Category_1']), product['Product_ID']), 1))
	
# 	if product['Product_Category_2'] != None:
# 		arr.append(((int(product['Product_Category_2']), product['Product_ID']), 1))

# 	if product['Product_Category_3'] != None:
# 		arr.append(((int(product['Product_Category_3']), product['Product_ID']), 1))

# 	return arr

# products_with_separate_category = data.rdd.flatMap(map_products_by_categories)
# products_cate_count = products_with_separate_category.reduceByKey(lambda a, b: a + b)
# products_cate_count = products_cate_count.map(lambda x: (x[0][0], [(x[0][1], x[1])]))
# products_cate_count_rdd = products_cate_count.reduceByKey(lambda a, b: a + b)
# # print(products_cate_count_rdd.collect())

# file = open("tmp_output_file/group_products_by_single_cate.ouput", 'w')
# for cate in products_cate_count_rdd.collect():
# 	file.write(str(cate[0]))
# 	file.write(';')

# 	products = cate[1]

# 	for index in range(0, len(products)):
# 		file.write(str(products[index][0]) + ':' + str(products[index][1]))
# 		if index != len(products) - 1:
# 			file.write(',')
# 	file.write('\n')
# file.close()

# READER
def map_group_by_category_file(row):
	cate = row.value.split(';')[0]
	result = {}
	products = row.value.split(';')[1].split(',')
	for index in range(0, len(products)):
		result[products[index].split(':')[0]] = int(products[index].split(':')[1])

	return (cate, result)

group_products_single_cate_file = spark.read.text("tmp_output_file/group_products_by_single_cate.ouput").rdd
group_products_single_cate = group_products_single_cate_file.map(map_group_by_category_file)
group_products_single_cate_dict = group_products_single_cate.collectAsMap()
print(group_products_single_cate_dict)