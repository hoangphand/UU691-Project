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
from pyspark.sql.functions import stddev

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")
test = spark.read.csv("dataset/test.csv", header = "true")

def pre_process_age(age):
	if age == "0-17":
		return 17
	if age == "18-25":
		return 25
	if age == "26-35":
		return 35
	if age == "36-45":
		return 45
	if age == "46-50":
		return 50
	if age == "51-55":
		return 55
	if age == "55+":
		return 60

# def pre_process_age_z(age):
def get_global_avg_age():
	return global_average_age_z

def get_global_age_stddev():
	return age_standard_deviation

def map_user_age(user_id):
	return user_distinct_map[user_id]

def pre_process_gender(gender):
	if gender == "F":
		return 1
	else:
		return 0

def pre_process_length_of_stay(length):
	if length == "0":
		return 0
	if length == "1":
		return 1
	if length == "2":
		return 2
	if length == "3":
		return 3
	if length == "4+":
		return 4

def map_products_by_categories(product):
	arr = []
	if product['Product_Category_1'] != None:
		arr.append((product['User_ID'], product['Product_ID'], product['Gender'], product['Age'], product['Occupation'], \
			product['City_Category'], product['Stay_In_Current_City_Years'], product['Marital_Status'], \
			int(product['Product_Category_1'])))
	
	if product['Product_Category_2'] != None:
		arr.append((product['User_ID'], product['Product_ID'], product['Gender'], product['Age'], product['Occupation'], \
			product['City_Category'], product['Stay_In_Current_City_Years'], product['Marital_Status'], \
			int(product['Product_Category_2'])))

	if product['Product_Category_3'] != None:
		arr.append((product['User_ID'], product['Product_ID'], product['Gender'], product['Age'], product['Occupation'], \
			product['City_Category'], product['Stay_In_Current_City_Years'], product['Marital_Status'], \
			int(product['Product_Category_3'])))

	return arr

udf_pre_process_age = udf(pre_process_age)
udf_pre_process_gender = udf(pre_process_gender)
udf_pre_process_length_of_stay = udf(pre_process_length_of_stay)
udf_get_global_avg_age = udf(get_global_avg_age)
udf_get_global_age_stddev = udf(get_global_age_stddev)
udf_map_user_age = udf(map_user_age)

Transaction = Row('User_ID', 'Product_ID', 'Gender', 'Age', 'Occupation', 'City_Category', 'Stay_In_Current_City_Years', \
	'Marital_Status', 'Product_Category')

training = training.rdd.flatMap(map_products_by_categories).map(lambda tx: Transaction(*tx)).distinct()
training = spark.createDataFrame(training)#.orderBy('User_ID', 'Product_Category')
training = training.groupBy('User_ID', 'Product_ID', 'Gender', 'Age', 'Occupation', 'City_Category', 'Stay_In_Current_City_Years', \
	'Marital_Status').agg(F.collect_list('Product_Category').alias('Product_Category')).orderBy('User_ID')
# training = training.withColumn("Product_Category", lit(None))
print(training.count())
training.show()

# user_distinct = training.select(training['User_ID'], training['Age']).orderBy('User_ID').distinct()
# user_distinct.show()
# user_distinct = user_distinct.withColumn("Age", udf_pre_process_age("Age").cast("int"))
# user_distinct_map = user_distinct.rdd.collectAsMap()
# no_of_users = user_distinct.count()
# global_average_age_z = user_distinct.groupBy().avg("Age").take(1)[0][0]
# age_standard_deviation = user_distinct.select(stddev("Age")).collect()[0][0]
# user_distinct.show()

# user_age_z = user_distinct.withColumn("Age-Z", (udf_map_user_age('User_ID') - udf_get_global_avg_age()) / udf_get_global_age_stddev())
# user_age_z.show()

# training = training.withColumn("Age", udf_pre_process_age("Age")) \
				# .withColumn("Gender", udf_pre_process_gender("Gender")) \
				# .withColumn("Stay_In_Current_City_Years", udf_pre_process_length_of_stay("Stay_In_Current_City_Years"))

# training.show()

# user_age_z.show()
# print(global_average_age_z.take(1)[0][0])
# print(age_standard_deviation)

# preprocess data
# first approach: convert groups of age to 

# data.show()