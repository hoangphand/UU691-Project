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

def append_to_dict_of_products(product):
	global dict_of_products
	# print(product.Product_ID)
	dict_of_products[str(product.Product_ID)] = 0
	# dict_of_products[str(product['Product_ID'])] = 0

def flatmap_candidate_pairs_in_basket(basket):
	result = []

	for pair in candidate_pairs:
		if set(pair).issubset(basket['Product_ID']):
			result.append((pair, 1))

	return result

udf_get_list_length = udf(get_list_length)
udf_add_order_id = udf(add_order_id)
udf_flatmap_candidate_pairs_in_basket = udf(flatmap_candidate_pairs_in_basket)

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

list_of_products = training.select('Product_ID').distinct().collect()

dict_of_products = {}
for product in list_of_products:
	dict_of_products[product['Product_ID']] = 0

training = training.withColumn('Order_ID', udf_add_order_id('User_ID'))

baskets = training.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID')).collect()

min_support = 0.001
min_confidence = 0.001

support_threshold = 1000
support_threshold_for_pair = 40

# FIRST PASS
for order in baskets:
	for product in order['Product_ID']:
		dict_of_products[product] += 1

# print(dict_of_products)

# SECOND PASS
frequent_single_items = []

for product in dict_of_products:
	if dict_of_products[product] > support_threshold:
		frequent_single_items.append(product)

candidate_pairs = list(itertools.combinations(frequent_single_items, 2))

candidate_pairs_count = {}

for pair in candidate_pairs:
	candidate_pairs_count[pair] = 0
	for basket in baskets:
		if set(pair).issubset(basket['Product_ID']):
			candidate_pairs_count[pair] += 1

frequent_pairs = []

for pair in candidate_pairs_count:
	if candidate_pairs_count[pair] > support_threshold_for_pair:
		frequent_pairs.append(pair)

print(frequent_pairs)