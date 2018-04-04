import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()

# WRITE

# READ group by product
# def map_group_by_product_file(line):
# 	product_cate = [int(i) for i in line.value.split(';')[1].split(',')]
# 	return (line.value.split(';')[0], product_cate)

# group_by_product_file = spark.read.text("tmp_output_file/group_by_product.ouput").rdd
# group_by_product = group_by_product_file.map(map_group_by_product_file)
# group_by_product_dict = group_by_product.collectAsMap()

# # READ tx with order id
# Order = Row('User_ID', 'Order_ID', 'Product_ID')
# def map_group_tx_add_order_id_to_dataset_file(line):
# 	user_id = line.value.split(';')[0]
# 	order_id = line.value.split(';')[1]
# 	product_id = line.value.split(';')[2].split(',')
# 	return (user_id, order_id, product_id)

# group_tx_add_order_id_file = spark.read.text("tmp_output_file/group_tx_add_order_id.ouput").rdd
# group_tx_add_order_id = group_tx_add_order_id_file.map(map_group_tx_add_order_id_to_dataset_file)

# def map_tx_set_cates(order):
# 	set_of_cates = []

# 	for product in order[2]:
# 		set_of_cates.append(group_by_product_dict[product])
	
# 	return (order[0], order[1], set_of_cates)

# group_tx_with_order_id_set_cates = group_tx_add_order_id.map(map_tx_set_cates)

# file = open("tmp_output_file/group_tx_with_order_id_set_cates.ouput", 'w')
# for order in group_tx_with_order_id_set_cates.collect():
# 	file.write(str(order[0]) + ';')
# 	file.write(str(order[1]) + ';')
# 	for index1 in range(0, len(order[2])):
# 		cates = list(order[2][index1])
# 		for index2 in range(0, len(cates)):
# 			file.write(str(cates[index2]))
# 			if index2 != len(cates) - 1:
# 				file.write(',')
# 		if index1 != len(order[2]) - 1:
# 			file.write(';')
# 	file.write('\n')
# file.close()

# READER
def map_group_tx_with_order_id_set_cates_file(line):
	user_id = int(line.value.split(';')[0])
	order_id = int(line.value.split(';')[1])
	set_of_cates = []
	for index in range(2, len(line.value.split(';'))):
		cates = [int(x) for x in line.value.split(';')[index].split(',')]
		set_of_cates.append(cates)

	return (user_id, order_id, set_of_cates)

Order_Cates = Row('User_ID', 'Order_ID', 'Category')
group_tx_with_order_id_set_cates_file = spark.read.text("tmp_output_file/group_tx_with_order_id_set_cates.ouput").rdd
group_tx_with_order_id_set_cates = group_tx_with_order_id_set_cates_file.map(map_group_tx_with_order_id_set_cates_file)\
	.map(lambda order: Order_Cates(*order))
# print(group_tx_with_order_id_set_cates.collect())
df = spark.createDataFrame(group_tx_with_order_id_set_cates)
df.show()