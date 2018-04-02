import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

# WRITER
spark = SparkSession.builder.appName("demo-cluster").getOrCreate()

# READ demographic users
Demographic_User = Row('Gender', 'Age', 'Occupation', 'User_ID')
def map_group_demographic_users_file(line):
	gender = line.value.split(';')[0].split(',')[0]
	age = line.value.split(';')[0].split(',')[1]
	occupation = line.value.split(';')[0].split(',')[2]
	user_id = line.value.split(';')[1].split(',')
	return (gender, age, occupation, user_id)

group_demographic_users_file = spark.read.text("tmp_output_file/group_demograhic_users.ouput").rdd
group_demographic_users = group_demographic_users_file.map(map_group_demographic_users_file)\
	.map(lambda row: Demographic_User(*row))
group_demographic_users = spark.createDataFrame(group_demographic_users)
group_demographic_users.show()

# READ group_user_products
def map_user_products_file(line):
	result = {}
	parts = line.value.split(';')
	# result['User_ID'] = parts[0]
	for index in range(1, len(parts) - 1):
		key = [int(i) for i in parts[index].split(':')[0].split(',')]
		value = int(parts[index].split(':')[1])
		result[tuple(key)] = int(value)

	return (parts[0], result)

user_products_file = spark.read.text("tmp_output_file/user_product_matrix_237.ouput").rdd
user_product_matrix_rdd = user_products_file.map(map_user_products_file)
user_product_matrix_dict = user_product_matrix_rdd.collectAsMap()

# GEN demographic products
first_user = user_product_matrix_rdd.take(1)[0][1]
keys = first_user.keys()

group_by_category_dict = {}

for key in keys:
	group_by_category_dict[key] = 0

def map_demographic_products(demographic):
	result = dict(group_by_category_dict)

	for user in demographic['User_ID']:
		user_cate_dict = user_product_matrix_dict[user]
		for cate in result:
			result[cate] += user_cate_dict[cate]

	return (tuple((demographic['Gender'], demographic['Age'], demographic['Occupation'])), result)

group_demographic_cate_rdd = group_demographic_users.rdd.map(map_demographic_products)
group_demographic_cate = group_demographic_cate_rdd.collect()

file = open("tmp_output_file/group_demographic_cate.ouput", 'w')
for demographic in group_demographic_cate:
	file.write(str(demographic[0][0]) + ",")
	file.write(str(demographic[0][1]) + ",")
	file.write(str(demographic[0][2]) + ",")
	file.write(';')
	keys = demographic[1].keys()
	for index1 in range(0, len(keys)):
		cates = list(keys[index1])
		for index2 in range(0, len(cates)):
			file.write(str(cates[index2]))
			if index2 != len(cates) - 1:
				file.write(',')
		file.write(':' + str(demographic[1][keys[index1]]))
		if index1 != len(keys) - 1:
			file.write(';')
	file.write('\n')
file.close()