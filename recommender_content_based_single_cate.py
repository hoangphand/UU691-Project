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
def map_recommended_single_cates_file(line):
	user_id = line.value.split(';')[0]
	cates = []
	parts = line.value.split(';')[1].split(',')
	for index in range(0, len(parts)):
		cates.append(int(parts[index]))
	return (user_id, cates)

recommended_single_cates_file = spark.read.text("tmp_output_file/recommended_single_cates_5.ouput").rdd
recommended_single_cates = recommended_single_cates_file.map(map_recommended_single_cates_file)
recommended_single_cates_dict = recommended_single_cates.collectAsMap()
print(recommended_single_cates_dict)
# print(recommended_cates_dict)

# READ group by products
def map_group_by_product_file(line):
	product_cate = [int(i) for i in line.value.split(';')[1].split(',')]
	return (line.value.split(';')[0], product_cate)

group_by_product_file = spark.read.text("tmp_output_file/group_by_product.ouput").rdd
group_by_product_dict = group_by_product_file.map(map_group_by_product_file).collectAsMap()

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
test_group_tx_add_order_id.show()

def evaluate_recommendation(user_id, actual_purchase_products):
	actual_purchased_cates = []

	for product in actual_purchase_products:
		actual_purchased_cates.extend(group_by_product_dict[product])

	recommended = recommended_single_cates_dict[user_id]
	intersection = set(actual_purchased_cates).intersection(recommended)
	result = list(intersection)
	if len(result) > 0:
		return 1
	else:
		return 0

udf_evaluate_recommendation = udf(evaluate_recommendation)

# add colum for evaluation
test_group_tx_add_order_id = test_group_tx_add_order_id.withColumn('Result', udf_evaluate_recommendation('User_ID', 'Product_ID'))\
	.orderBy('User_ID', 'Order_ID')

print(test_group_tx_add_order_id.rdd.filter(lambda row: int(row['Result']) == 1).count())