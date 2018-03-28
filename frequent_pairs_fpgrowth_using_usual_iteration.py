import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import grouping
from pyspark.sql.functions import mean
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
from pyspark.sql.functions import stddev
from pyspark.ml.fpm import FPGrowth
import itertools

def get_list_length(list):
	return len(list)

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

def flatmap_candidate_pairs_in_basket(basket):
	result = []

	for pair in candidate_pairs:
		if set(pair).issubset(basket['Product_ID']):
			result.append((pair, 1))

	return result

def sort_product_ids(product_ids):
	return sorted(product_ids)

udf_get_list_length = udf(get_list_length)
udf_add_order_id = udf(add_order_id)
udf_flatmap_candidate_pairs_in_basket = udf(flatmap_candidate_pairs_in_basket)
udf_sort_product_ids = udf(sort_product_ids)

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

list_of_products = training.select('Product_ID').distinct().collect()

dict_of_products = {}
for product in list_of_products:
	dict_of_products[str(product['Product_ID'])] = 0

training = training.withColumn('Order_ID', udf_add_order_id('User_ID'))

baskets = training.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID'))

baskets.withColumn('Product_ID', udf_sort_product_ids('Product_ID'))
baskets = baskets.collect()
# baskets = baskets.withColumn('Product_ID', udf_sort_product_ids('Product_ID')).collect()

for i in range(0, 10):
	print(baskets[i])

min_support = 0.001
min_confidence = 0.001

support_threshold = 1000
support_threshold_for_pair = 40

# PASS 1:
# for order in baskets:
# 	for product in order['Product_ID']:
# 		# if dict_of_products[product]
# 		# dict_of_products[str(product)] += 1
# 		print(product)

# frequent_single_items = []

# for product in dict_of_products:
# 	if dict_of_products[product] > support_threshold:
# 		frequent_single_items.append(product)