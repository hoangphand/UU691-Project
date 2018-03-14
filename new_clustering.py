from __future__ import print_function
import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import random
import copy
import math

random_seed = 1992
random.seed(random_seed)

def map_user_product_dict(user_product):
	global distinct_products_dict
	tmp_result = dict(distinct_products_dict)

	tmp_result['User_ID'] = user_product['User_ID']

	for product in user_product['Product_ID']:
		tmp_result[product] = 1

	return tmp_result

def update_centroid_average():
	global centroid_vectors
	global K

	for index in range(0, K):
		cluster_size = len(clusters[index])
		users_in_cluster = matrix_user_product_rdd.filter(lambda user: user['User_ID'] in clusters[index]).collect()
		for key in centroid_vectors[index]:
			current_sum = 0
			if key != 'User_ID':
				for user in users_in_cluster:
					current_sum += user[key]
			centroid_vectors[index][key] = current_sum / cluster_size

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

distinct_products = training.select(training['Product_ID']).distinct()
distinct_products_dict = distinct_products.rdd.map(lambda product: (product[0], 0)).collectAsMap()

user_distinct = training.select('User_ID').distinct().rdd.map(lambda user: user['User_ID']).collect()
# print(user_distinct.count())
# print(user_distinct)

user_product = training.select('User_ID', 'Product_ID').distinct().groupBy('User_ID')\
	.agg(F.collect_list('Product_ID').alias('Product_ID'))

matrix_user_product_rdd = user_product.rdd.map(map_user_product_dict)
matrix_user_product = matrix_user_product_rdd.collect()

# Clustering based on number of cities K = 3
K = 20
clusters = [[]] * K
random_initial_user = random.sample(user_distinct, K)

centroid_vectors = matrix_user_product_rdd.filter(lambda user: user["User_ID"] in random_initial_user).collect()

# for user_vector in matrix_user_product:
# 	current_min_distance = sys.maxsize
# 	min_index = -1

# 	for index in range(0, K):
# 		current_distance = 0
# 		for key in centroid_vectors[index]:
# 			if key != 'User_ID':
# 				current_distance += math.pow(abs(centroid_vectors[index][key] - user_vector[key]), 2)

# 		if current_distance < current_min_distance:
# 			current_min_distance = current_distance
# 			min_index = index

# 	if len(clusters[min_index]) == 0:
# 		clusters[min_index] = []

# 	clusters[min_index].append(user_vector['User_ID'])

last_centroid_vectors = []

iteration = 0

while centroid_vectors != last_centroid_vectors:
	last_centroid_vectors = copy.deepcopy(centroid_vectors)
	clusters = [[]] * K

	iteration += 1
	print("iteration: " + str(iteration))

	for user_vector in matrix_user_product:
		current_min_distance = sys.maxsize
		min_index = -1

		for index in range(0, K):
			current_distance = 0
			for key in centroid_vectors[index]:
				if key != 'User_ID':
					current_distance += math.pow(abs(centroid_vectors[index][key] - user_vector[key]), 2)

			if current_distance < current_min_distance:
				current_min_distance = current_distance
				min_index = index

		if len(clusters[min_index]) == 0:
			clusters[min_index] = []

		clusters[min_index].append(user_vector['User_ID'])

	for index in range(0, K):
		cluster_size = len(clusters[index])
		print("cluster " + str(index) + " size: " + str(cluster_size))
	update_centroid_average()

file = open("output_file", "w")

for index in range(0, K):
	file.write("* CLASS " + str(index) + "\n")
	# print("* CLASS " + str(index))

	for el in clusters[index]:
		file.write(el + ' ')
		# print(el, end=' ')
	# print()
	file.write("\n")
file.close()