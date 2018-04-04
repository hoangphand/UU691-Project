import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
import random
import operator

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")
test = spark.read.csv("dataset/test.csv", header = "true")

# READ group by cate
def map_group_by_category_file(row):
	cate = row.value.split(';')[0]
	result = {}
	products = row.value.split(';')[1].split(',')
	for index in range(0, len(products)):
		result[products[index].split(':')[0]] = int(products[index].split(':')[1])

	return (int(cate), result)

group_products_single_cate_file = spark.read.text("tmp_output_file/group_products_by_single_cate.ouput").rdd
group_products_single_cate = group_products_single_cate_file.map(map_group_by_category_file)
group_products_single_cate_dict = group_products_single_cate.collectAsMap()
# print(group_products_single_cate_dict)

# READ user matrix
def map_recommended_single_cates_file(line):
	user_id = line.value.split(';')[0]
	cates = []
	parts = line.value.split(';')[1].split(',')
	for index in range(0, len(parts)):
		cates.append(int(parts[index]))
	return (user_id, cates)

recommended_single_cates_file = spark.read.text("tmp_output_file/recommended_single_cates_10.ouput").rdd
recommended_single_cates = recommended_single_cates_file.map(map_recommended_single_cates_file)
# recommended_single_cates_dict = recommended_single_cates.collectAsMap()
# print(recommended_single_cates_dict)

# GEN recommended products
no_of_products_for_each_cate = 3
random_seed = 191
random.seed(random_seed)

def map_recommended_products(user):
	user_recommended_products = []

	for cate in user[1]:
		recommended_products = sorted(group_products_single_cate_dict[cate].iteritems(), key=operator.itemgetter(1), reverse=True)[:no_of_products_for_each_cate]
		user_recommended_products.extend([x[0] for x in recommended_products])

	return (user[0], user_recommended_products)

recommended_products = recommended_single_cates.map(map_recommended_products).collect()

file = open("tmp_output_file/recommended_products_single_cate_10x3.ouput", 'w')
for row in recommended_products:
	file.write(str(row[0]))
	file.write(';')

	for index in range(0, len(row[1])):
		file.write(str(row[1][index]))
		if index != len(row[1]) - 1:
			file.write(',')
	file.write('\n')
file.close()


# READER
# def map_one_time_recommended_products_file(line):
# 	user_id = line.value.split(';')[0]
# 	product = line.value.split(';')[1].split(',')
# 	return (user_id, product)

# one_time_recommended_products_file = spark.read.text("tmp_output_file/one_time_recommended_products_237_3x3_191.ouput").rdd
# one_time_recommended_products = one_time_recommended_products_file.map(map_one_time_recommended_products_file)
# one_time_recommended_products_dict = one_time_recommended_products.collectAsMap()
# print(one_time_recommended_products_dict)