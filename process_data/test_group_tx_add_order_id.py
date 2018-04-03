import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
test = spark.read.csv("dataset/test.csv", header = "true")

# WRITER
new_products = spark.read.text("tmp_output_file/new_products_in_test_set.output").rdd.map(lambda line: line.value).collect()

global_order_count = 0
last_user_id = -1

def add_order_id(current_user_id):
	global last_user_id
	global global_order_count
	if last_user_id == -1:
		last_user_id = current_user_id
		global_order_count = 1
		return global_order_count

	if current_user_id != last_user_id:
		last_user_id = current_user_id
		global_order_count = global_order_count + 1

	return global_order_count

udf_add_order_id = udf(add_order_id)

dataset_with_order_id = test.withColumn('Order_ID', udf_add_order_id('User_ID'))

user_order = dataset_with_order_id.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
	.filter(~dataset_with_order_id['Product_ID'].isin(new_products))\
	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID'))\
	.orderBy('User_ID', 'Order_ID')

file = open("tmp_output_file/test_group_tx_add_order_id.ouput", 'w')
for row in user_order.collect():
	file.write(str(row['User_ID']))
	file.write(';')
	file.write(str(row['Order_ID']))
	file.write(';')

	for index in range(0, len(row['Product_ID'])):
		file.write(str(row['Product_ID'][index]))
		if index != len(row['Product_ID']) - 1:
			file.write(',')
	file.write('\n')
file.close()

# READER
# Order = Row('User_ID', 'Order_ID', 'Product_ID')
# def map_test_group_tx_add_order_id_to_dataset_file(line):
# 	user_id = line.value.split(';')[0]
# 	order_id = line.value.split(';')[1]
# 	product_id = line.value.split(';')[2].split(',')
# 	return (user_id, order_id, product_id)

# test_group_tx_add_order_id_file = spark.read.text("tmp_output_file/test_group_tx_add_order_id.ouput").rdd
# test_group_tx_add_order_id = test_group_tx_add_order_id_file.map(map_test_group_tx_add_order_id_to_dataset_file)\
# 	.map(lambda order: Order(*order))
# test_group_tx_add_order_id = spark.createDataFrame(test_group_tx_add_order_id)
# test_group_tx_add_order_id.show()