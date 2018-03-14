from __future__ import print_function
import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import random
import copy
import math
# from random import *

random_seed = 1992
# random_seed = randint(1, 10000)
random.seed(random_seed)
# print("Random seed: " + str(random_seed))

def map_products_by_categories(product):
	arr = []
	if product['Product_Category_1'] != None:
		arr.append(((product['User_ID'], int(product['Product_Category_1'])), 1))
	
	if product['Product_Category_2'] != None:
		arr.append(((product['User_ID'], int(product['Product_Category_2'])), 1))

	if product['Product_Category_3'] != None:
		arr.append(((product['User_ID'], int(product['Product_Category_3'])), 1))

	return arr

def map_user_products_dict(user_product):
	result = {}
	result['User_ID'] = user_product[0]
	for cate in range(1, 21):
		result[cate] = 0

	for cate in user_product[1]:
		result[cate[0]] = cate[1]

	return result

spark = SparkSession.builder.appName("basic-analytics").getOrCreate()
data = spark.read.csv("dataset/train.csv", header = "true")

products_with_separate_category = data.rdd.flatMap(map_products_by_categories)


user_product_purchased_count = products_with_separate_category.reduceByKey(lambda a, b: a + b)
user_product_purchased_count = user_product_purchased_count.map(lambda x: (x[0][0], [(x[0][1], x[1])]))

user_purchased_products_by_cate_rdd = user_product_purchased_count.reduceByKey(lambda a, b: a + b)

matrix_user_vector_rdd = user_purchased_products_by_cate_rdd.map(map_user_products_dict)
matrix_user_vector = matrix_user_vector_rdd.collect()

def update_centroid_average():
	global centroid_vectors
	global K

	for index in range(0, K):
		cluster_size = len(clusters[index])
		users_in_cluster = matrix_user_vector_rdd.filter(lambda user: user['User_ID'] in clusters[index]).collect()
		for key in centroid_vectors[index]:
			current_sum = 0
			if key != 'User_ID':
				for user in users_in_cluster:
					current_sum += user[key]
			centroid_vectors[index][key] = current_sum / cluster_size

user_distinct = data.select('User_ID').distinct().rdd.map(lambda user: user['User_ID']).collect()

K = 3

clusters = [[]] * K
random_initial_user = random.sample(user_distinct, K)

centroid_vectors = matrix_user_vector_rdd.filter(lambda user: user['User_ID'] in random_initial_user).collect()

for user_vector in matrix_user_vector:
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

last_centroid_vectors = []

iteration = 0

while centroid_vectors != last_centroid_vectors:
	last_centroid_vectors = copy.deepcopy(centroid_vectors)
	clusters = [[]] * K

	iteration += 1
	print("iteration: " + str(iteration))

	for user_vector in matrix_user_vector:
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

	for el in clusters[index]:
		file.write(el + ' ')
	file.write("\n")
file.close()

for index in range(0, K):
	cityA = data.select('User_ID', 'City_Category').distinct()\
		.rdd.filter(lambda user: user['City_Category'] == 'A' and user['User_ID'] in clusters[index]).count()
	perA = cityA / len(clusters[index])
	cityB = data.select('User_ID', 'City_Category').distinct()\
		.rdd.filter(lambda user: user['City_Category'] == 'B' and user['User_ID'] in clusters[index]).count()
	perB = cityB / len(clusters[index])
	cityC = data.select('User_ID', 'City_Category').distinct()\
		.rdd.filter(lambda user: user['City_Category'] == 'C' and user['User_ID'] in clusters[index]).count()
	perC = cityC / len(clusters[index])

	print("Cluster " + str(index) + " size: " + str(len(clusters[index])))
	print("A: " + str(cityA) + "/" + str(len(clusters[index])) + " = " + str(perA) + "%")
	print("B: " + str(cityB) + "/" + str(len(clusters[index])) + " = " + str(perB) + "%")
	print("C: " + str(cityC) + "/" + str(len(clusters[index])) + " = " + str(perC) + "%")
