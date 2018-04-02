import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
import operator

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")
test = spark.read.csv("dataset/test.csv", header = "true")

# READ group by cate
def map_group_by_category_file(line):
	cate = [int(i) for i in line.value.split(';')[0].split(',')]
	product = line.value.split(';')[1].split(',')
	return (tuple(cate), product)

group_by_category_file = spark.read.text("tmp_output_file/group_by_category.ouput").rdd
group_by_category = group_by_category_file.map(map_group_by_category_file)
group_by_category_dict = group_by_category.collectAsMap()

# READ demographic cate
def map_demographic_cate_file(line):
	result = {}
	parts = line.value.split(';')
	gender = parts[0].split(',')[0]
	age = parts[0].split(',')[1]
	occupation = parts[0].split(',')[2]
	marital_status = parts[0].split(',')[3]
	duration_of_stay = parts[0].split(',')[4]
	city = parts[0].split(',')[5]
	for index in range(1, len(parts) - 1):
		key = [int(i) for i in parts[index].split(':')[0].split(',')]
		value = int(parts[index].split(':')[1])
		result[tuple(key)] = int(value)

	return (tuple((gender, age, occupation, marital_status, duration_of_stay, city)), result)

demographic_cate_file = spark.read.text("tmp_output_file/group_demographic_cate.ouput").rdd
demographic_cate_rdd = demographic_cate_file.map(map_demographic_cate_file)
demographic_cate_dict = demographic_cate_rdd.collectAsMap()

# GEN recommended products
def map_collect_user_profile(user):
	return (user['User_ID'], user['Gender'], user['Age'], user['Occupation'], user['Marital_Status'], user['Stay_In_Current_City_Years'], user['City_Category'])
User = Row('User_ID', 'Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years', 'City_Category')
distinct_users = training.rdd.map(map_collect_user_profile).map(lambda user: User(*user)).distinct()

no_of_cates = 6
no_of_products_for_each_cate = 6

def map_recommended_products(user):
	key = tuple((user['Gender'], user['Age'], user['Occupation'], user['Marital_Status'], user['Stay_In_Current_City_Years'], user['City_Category']))
	top_user_cates = sorted(demographic_cate_dict[key].iteritems(), key=operator.itemgetter(1), reverse=True)
	user_recommended_products = []

	max_no_of_products = no_of_products_for_each_cate * no_of_cates
	current_total = 0
	count = 0

	while current_total < max_no_of_products:
		max_to_be_chosen = min(len(group_by_category_dict[top_user_cates[count][0]]), no_of_products_for_each_cate)
		to_be_chosen = min(max_to_be_chosen, max_no_of_products - current_total)
		if to_be_chosen == 0:
			break
		current_total += to_be_chosen
		for index in range(0, to_be_chosen):
			user_recommended_products.append(group_by_category_dict[top_user_cates[count][0]][index])
		count += 1

	return (user['User_ID'], user_recommended_products)

recommended_products = distinct_users.map(map_recommended_products).collect()

file = open("tmp_output_file/one_time_recommended_products_demo_cate_6x6.ouput", 'w')
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