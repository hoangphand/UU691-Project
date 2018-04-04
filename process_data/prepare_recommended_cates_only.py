import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
import operator

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()

# WRITE
def map_user_products_file(line):
	result = {}
	parts = line.value.split(';')
	for index in range(1, len(parts) - 1):
		key = [int(i) for i in parts[index].split(':')[0].split(',')]
		value = int(parts[index].split(':')[1])
		result[tuple(key)] = int(value)

	return (parts[0], result)

user_products_file = spark.read.text("tmp_output_file/user_product_matrix_237.ouput").rdd
user_product_matrix_rdd = user_products_file.map(map_user_products_file)

# GEN recommended cates
no_of_cates = 6

def map_recommended_cates(user):
	top_user_cates = sorted(user[1].iteritems(), key=operator.itemgetter(1), reverse=True)[:no_of_cates]

	user_recommended_cates = []

	for cate in top_user_cates:
		user_recommended_cates.append(cate[0])

	return (user[0], user_recommended_cates)

recommended_cates = user_product_matrix_rdd.map(map_recommended_cates).collect()
file = open("tmp_output_file/recommended_cates_237_6.ouput", 'w')
for row in recommended_cates:
	file.write(str(row[0]))
	file.write(';')

	for index1 in range(0, len(row[1])):
		key = list(row[1][index1])
		for index2 in range(0, len(key)):
			file.write(str(key[index2]))
			if index2 != len(key) - 1:
				file.write(',')
		if index1 != len(row[1]) - 1:
			file.write(';')
	file.write('\n')
file.close()

# READER
# def map_recommended_cates_file(line):
# 	parts = line.value.split(';')
# 	user_id = parts[0]
# 	cates = []
# 	for index in range(1, len(parts)):
# 		cates.append(tuple([int(x) for x in parts[index].split(',')]))
# 	return (user_id, cates)

# recommended_cates_file = spark.read.text("tmp_output_file/recommended_cates_237_5.ouput").rdd
# recommended_cates = recommended_cates_file.map(map_recommended_cates_file)
# recommended_cates_dict = recommended_cates.collectAsMap()
# print(recommended_cates_dict)