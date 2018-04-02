import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

# WRITER
# Order = Row('User_ID', 'Order_ID', 'Product_ID')
# def map_group_tx_add_order_id_to_dataset_file(line):
# 	user_id = line.value.split(';')[0]
# 	order_id = line.value.split(';')[1]
# 	product_id = line.value.split(';')[2].split(',')
# 	return (user_id, order_id, product_id)

# group_tx_add_order_id_file = spark.read.text("tmp_output_file/group_tx_add_order_id.ouput").rdd
# group_tx_add_order_id = group_tx_add_order_id_file.map(map_group_tx_add_order_id_to_dataset_file)\
# 	.map(lambda order: Order(*order))
# group_tx_add_order_id = spark.createDataFrame(group_tx_add_order_id)
# group_tx_add_order_id.show()

# user_products_without_order_id = group_tx_add_order_id.select('User_ID', 'Product_ID')\
# 	.groupBy('User_ID').agg(F.collect_list('Product_ID').alias('Product_ID')).orderBy('User_ID')

# file = open("tmp_output_file/group_user_products.ouput", 'w')
# for row in user_products_without_order_id.collect():
# 	file.write(str(row['User_ID']))
# 	file.write(';')

# 	for index1 in range(0, len(row['Product_ID'])):
# 		for index2 in range(0, len(row['Product_ID'][index1])):
# 			file.write(str(row['Product_ID'][index1][index2]))
# 			if index2 != len(row['Product_ID'][index1]) - 1:
# 				file.write(',')
# 		file.write(';')
# 	file.write('\n')
# file.close()

# READER
User_Products = Row('User_ID', 'Product_ID')
def map_group_user_products_file(line):
	parts = line.value.split(';')
	user_id = parts[0]
	products = []
	for index in range(1, len(parts)):
		products.append(parts[index].split(','))
	return (user_id, products)

group_user_products_file = spark.read.text("tmp_output_file/group_user_products.ouput").rdd
group_user_products = group_user_products_file.map(map_group_user_products_file)\
	.map(lambda user_products: User_Products(*user_products))
group_user_products = spark.createDataFrame(group_user_products)
group_user_products.show()