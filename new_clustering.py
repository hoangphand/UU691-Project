import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import random

random_seed = 191
random.seed(random_seed)

def map_user_product_dict(user_product):
	global distinct_products_dict
	tmp_result = dict(distinct_products_dict)

	tmp_result['User_ID'] = user_product['User_ID']

	for product in user_product['Product_ID']:
		tmp_result[product] = 1

	return tmp_result

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

distinct_products = training.select(training['Product_ID']).distinct()
distinct_products_dict = distinct_products.rdd.map(lambda product: (product[0], 0)).collectAsMap()

user_distinct = training.select('User_ID').distinct().rdd.map(lambda user: user['User_ID']).collect()
# print(user_distinct.count())
# print(user_distinct)

user_product = training.select('User_ID', 'Product_ID').distinct().groupBy('User_ID')\
	.agg(F.collect_list('Product_ID').alias('Product_ID'))

matrix_user_product = user_product.rdd.map(map_user_product_dict)

# # Clustering based on number of cities
# # K = 3
# K = 3
clusters = [[]] * K
random_initial_user = random.sample(user_distinct, K)

centroid_vectors = matrix_rdd.filter(lambda state: state["name"] in initial_states).