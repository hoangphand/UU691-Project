import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")
	
# WRITE
# def map_construct_user_product_matrix(user):
# 	tmp_matrix = dict(group_by_category_dict)
# 	tmp_matrix['User_ID'] = user['User_ID']

# 	if user['User_ID'] == '1000001':
# 		print(user['Product_ID'])
# 	for basket in user['Product_ID']:
# 		for product in basket:
# 			product_cate = tuple(group_by_product_dict[product])
# 			tmp_matrix[product_cate] += 1

# 	return tmp_matrix


# # READ group_by_product
# Product = Row('Product_ID', 'Product_Category')
# def map_group_by_product_file(line):
# 	product_cate = [int(i) for i in line.value.split(';')[1].split(',')]
# 	return (line.value.split(';')[0], product_cate)

# group_by_product_file = spark.read.text("tmp_output_file/group_by_product.ouput").rdd
# group_by_product = group_by_product_file.map(map_group_by_product_file).map(lambda product: Product(*product))
# group_by_product = spark.createDataFrame(group_by_product)

# # READ group_by_category
# Cate_Product = Row('Product_Category', 'Product_ID')
# def map_group_by_category_file(line):
# 	cate = [int(i) for i in line.value.split(';')[0].split(',')]
# 	product = line.value.split(';')[1].split(',')
# 	return (cate, product)

# group_by_category_file = spark.read.text("tmp_output_file/group_by_category.ouput").rdd
# group_by_category = group_by_category_file.map(map_group_by_category_file).map(lambda cate: Cate_Product(*cate))
# group_by_category = spark.createDataFrame(group_by_category)

# # GEN dicts
# group_by_product_dict = group_by_product.rdd.map(lambda product: (product['Product_ID'], product['Product_Category']))\
# 	.collectAsMap()
# group_by_category_dict = group_by_category.rdd.map(lambda cate: (tuple(cate['Product_Category']), 0)).collectAsMap()

# # READ group_user_products
# User_Products = Row('User_ID', 'Product_ID')
# def map_group_user_products_file(line):
# 	parts = line.value.split(';')
# 	user_id = parts[0]
# 	products = []
# 	for index in range(1, len(parts) - 1):
# 		products.append(parts[index].split(','))
# 	return (user_id, products)

# group_user_products_file = spark.read.text("tmp_output_file/group_user_products.ouput").rdd
# group_user_products = group_user_products_file.map(map_group_user_products_file)\
# 	.map(lambda user_products: User_Products(*user_products))
# group_user_products = spark.createDataFrame(group_user_products)

# user_product_matrix_rdd = group_user_products.rdd.map(map_construct_user_product_matrix)
# user_product_matrix = user_product_matrix_rdd.collect()

# file = open("tmp_output_file/user_product_matrix_237.ouput", 'w')
# for user in user_product_matrix:
# 	file.write(str(user['User_ID']))
# 	file.write(';')
# 	keys = user.keys()
# 	for index1 in range(0, len(keys)):
# 		if keys[index1] != 'User_ID':
# 			cates = list(keys[index1])
# 			for index2 in range(0, len(cates)):
# 				file.write(str(cates[index2]))
# 				if index2 != len(cates) - 1:
# 					file.write(',')
# 			file.write(':' + str(user[keys[index1]]))
# 			if index1 != len(keys) - 1:
# 				file.write(';')
# 	file.write('\n')
# file.close()

# READER
def map_user_products_file(line):
	result = {}
	parts = line.value.split(';')
	result['User_ID'] = parts[0]
	for index in range(1, len(parts) - 1):
		key = [int(i) for i in parts[index].split(':')[0].split(',')]
		value = int(parts[index].split(':')[1])
		result[tuple(key)] = int(value)

	return result

user_products_file = spark.read.text("tmp_output_file/user_product_matrix_237.ouput").rdd
user_product_matrix_rdd = user_products_file.map(map_user_products_file)
user_product_matrix = user_product_matrix_rdd.collect()