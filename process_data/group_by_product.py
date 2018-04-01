import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

# WRITER
spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")
Product = Row('Product_ID', 'Product_Category')

# def map_products_by_categories(product):
# 	arr = []
# 	if product['Product_Category_1'] != None:
# 		arr.append((product['Product_ID'], int(product['Product_Category_1'])))
	
# 	if product['Product_Category_2'] != None:
# 		arr.append((product['Product_ID'], int(product['Product_Category_2'])))

# 	if product['Product_Category_3'] != None:
# 		arr.append((product['Product_ID'], int(product['Product_Category_3'])))

# 	return arr

# rdd = training.rdd.flatMap(map_products_by_categories).map(lambda product: Product(*product)).distinct()

# group_by_product = spark.createDataFrame(rdd).orderBy('Product_Category')\
# 	.groupBy('Product_ID').agg(F.collect_list('Product_Category').alias('Product_Category')).orderBy('Product_ID')
# file = open("tmp_output_file/group_by_product.ouput", 'w')
# for product in group_by_product.collect():
# 	file.write(product['Product_ID'] + ';')
# 	for index in range(0, len(product['Product_Category'])):
# 		# print(product['Product_Category'][index])
# 		file.write(str(product['Product_Category'][index]))
# 		if index != len(product['Product_Category']) - 1:
# 			file.write(',')
# 	file.write('\n')
# file.close()
# group_by_product.show()
# print(group_by_product.count())

# READER
def map_group_by_product_file(line):
	product_cate = [int(i) for i in line.value.split(';')[1].split(',')]
	return (line.value.split(';')[0], product_cate)

group_by_product_file = spark.read.text("tmp_output_file/group_by_product.ouput").rdd
group_by_product = group_by_product_file.map(map_group_by_product_file).map(lambda product: Product(*product))
group_by_product = spark.createDataFrame(group_by_product)
group_by_product.show()