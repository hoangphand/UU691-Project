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

udf_add_order_id = udf(add_order_id)
udf_flatmap_candidate_pairs_in_basket = udf(flatmap_candidate_pairs_in_basket)

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

min_support = 0.001
min_confidence = 0.001

support_threshold = 1000
support_threshold_for_pair = 40

# FIRST PASS
products_count = training.rdd.map(lambda product: (product['Product_ID'], 1)).reduceByKey(lambda a, b: a + b)

# SECOND PASS
frequent_single_items_with_count = products_count.filter(lambda product: product[1] > support_threshold)
frequent_single_items = frequent_single_items_with_count.map(lambda product: product[0])

candidate_pairs = list(itertools.combinations(frequent_single_items.collect(), 2))

training = training.withColumn('Order_ID', udf_add_order_id('User_ID'))
baskets = training.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID'))

candidate_count = baskets.rdd.flatMap(flatmap_candidate_pairs_in_basket).reduceByKey(lambda a, b: a + b)

true_frequent_pair = candidate_count.filter(lambda pair: pair[1] > support_threshold_for_pair).collect()

print(true_frequent_pair)