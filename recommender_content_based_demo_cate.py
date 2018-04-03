import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")
test = spark.read.csv("dataset/test.csv", header = "true")

# GEN one time recommended products
def map_one_time_recommended_products_file(line):
	user_id = line.value.split(';')[0]
	product = line.value.split(';')[1].split(',')
	return (user_id, product)

one_time_recommended_products_file = spark.read.text("tmp_output_file/one_time_recommended_products_demo_cate_6x6.ouput").rdd
one_time_recommended_products = one_time_recommended_products_file.map(map_one_time_recommended_products_file)
one_time_recommended_products_dict = one_time_recommended_products.collectAsMap()

# GEN orders with actual products purchased from test
Order = Row('User_ID', 'Order_ID', 'Product_ID')
def map_test_group_tx_add_order_id_to_dataset_file(line):
	user_id = line.value.split(';')[0]
	order_id = line.value.split(';')[1]
	product_id = line.value.split(';')[2].split(',')
	return (user_id, order_id, product_id)

test_group_tx_add_order_id_file = spark.read.text("tmp_output_file/test_group_tx_add_order_id.ouput").rdd
test_group_tx_add_order_id = test_group_tx_add_order_id_file.map(map_test_group_tx_add_order_id_to_dataset_file)\
	.map(lambda order: Order(*order))
test_group_tx_add_order_id = spark.createDataFrame(test_group_tx_add_order_id)

def evaluate_recommendation(user_id, actual_purchase_products):
	result = list(set(actual_purchase_products).intersection(one_time_recommended_products_dict[user_id]))
	if len(result) > 0:
		return 1
	else:
		return 0

udf_evaluate_recommendation = udf(evaluate_recommendation)

# add colum for evaluation
test_group_tx_add_order_id = test_group_tx_add_order_id.withColumn('Result', udf_evaluate_recommendation('User_ID', 'Product_ID'))\
	.orderBy('User_ID', 'Order_ID')

print(test_group_tx_add_order_id.rdd.filter(lambda row: int(row['Result']) == 1).count())
#result = test_group_tx_add_order_id.groupBy().sum('Result').collect()
#print(len(result.rdd.filter(lambda basket: basket['Result'] == 1).collect()))