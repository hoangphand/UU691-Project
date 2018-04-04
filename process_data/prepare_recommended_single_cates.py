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
def map_group_by_category_file(row):
	user_id = row.value.split(';')[0]
	result = {}
	cates = row.value.split(';')[1].split(',')
	for index in range(0, len(cates)):
		result[int(cates[index].split(':')[0])] = int(cates[index].split(':')[1])

	return (user_id, result)

group_user_single_cate_file = spark.read.text("tmp_output_file/group_user_single_cate.ouput").rdd
group_user_single_cate_rdd = group_user_single_cate_file.map(map_group_by_category_file)

# GEN recommended cates
no_of_cates = 6

def map_recommended_cates(user):
	top_user_cates = sorted(user[1].iteritems(), key=operator.itemgetter(1), reverse=True)[:no_of_cates]

	user_recommended_cates = []

	for cate in top_user_cates:
		user_recommended_cates.append(cate[0])

	return (user[0], user_recommended_cates)

recommended_cates = group_user_single_cate_rdd.map(map_recommended_cates).collect()
file = open("tmp_output_file/recommended_single_cates_6.ouput", 'w')
for row in recommended_cates:
	file.write(str(row[0]))
	file.write(';')

	for index in range(0, len(row[1])):
		file.write(str(row[1][index]))
		if index != len(row[1]) - 1:
			file.write(',')
	file.write('\n')
file.close()

# READER
# def map_recommended_single_cates_file(line):
# 	user_id = line.value.split(';')[0]
# 	cates = []
# 	parts = line.value.split(';')[1].split(',')
# 	for index in range(0, len(parts)):
# 		cates.append(int(parts[index]))
# 	return (user_id, cates)

# recommended_single_cates_file = spark.read.text("tmp_output_file/recommended_single_cates_4.ouput").rdd
# recommended_single_cates = recommended_single_cates_file.map(map_recommended_single_cates_file)
# recommended_single_cates_dict = recommended_single_cates.collectAsMap()
# print(recommended_single_cates_dict)