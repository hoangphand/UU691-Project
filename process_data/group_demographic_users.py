import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

# WRITER
spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

# demographic_distinct = training.select('User_ID', 'Gender', 'Age', 'Occupation')\
# 	.distinct().groupBy('Gender', 'Age', 'Occupation')\
# 	.agg(F.collect_list('User_ID').alias('User_ID'))\
# 	.orderBy('Gender', 'Age', 'Occupation')
# demographic_distinct.show()

# file = open("tmp_output_file/group_demograhic_users.ouput", 'w')
# for row in demographic_distinct.collect():
# 	file.write(row['Gender'] + ',')
# 	file.write(row['Age'] + ',')
# 	file.write(row['Occupation'])
# 	file.write(';')

# 	for index in range(0, len(row['User_ID'])):
# 		file.write(row['User_ID'][index])
# 		if index != len(row['User_ID']) - 1:
# 			file.write(',')
# 	file.write('\n')
# file.close()

# demographic_distinct = training.select('User_ID', 'Gender', 'Age', 'Occupation', 'Marital_Status')\
# 	.distinct().groupBy('Gender', 'Age', 'Occupation', 'Marital_Status')\
# 	.agg(F.collect_list('User_ID').alias('User_ID'))\
# 	.orderBy('Gender', 'Age', 'Occupation', 'Marital_Status')
# demographic_distinct.show()

# file = open("tmp_output_file/group_demograhic_users.ouput", 'w')
# for row in demographic_distinct.collect():
# 	file.write(row['Gender'] + ',')
# 	file.write(row['Age'] + ',')
# 	file.write(row['Occupation'] + ',')
# 	file.write(row['Marital_Status'])
# 	file.write(';')

# 	for index in range(0, len(row['User_ID'])):
# 		file.write(row['User_ID'][index])
# 		if index != len(row['User_ID']) - 1:
# 			file.write(',')
# 	file.write('\n')
# file.close()

# demographic_distinct = training.select('User_ID', 'Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years')\
# 	.distinct().groupBy('Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years')\
# 	.agg(F.collect_list('User_ID').alias('User_ID'))\
# 	.orderBy('Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years')
# demographic_distinct.show()

# file = open("tmp_output_file/group_demograhic_users.ouput", 'w')
# for row in demographic_distinct.collect():
# 	file.write(row['Gender'] + ',')
# 	file.write(row['Age'] + ',')
# 	file.write(row['Occupation'] + ',')
# 	file.write(row['Marital_Status'] + ',')
# 	file.write(row['Stay_In_Current_City_Years'])
# 	file.write(';')

# 	for index in range(0, len(row['User_ID'])):
# 		file.write(row['User_ID'][index])
# 		if index != len(row['User_ID']) - 1:
# 			file.write(',')
# 	file.write('\n')
# file.close()

demographic_distinct = training.select('User_ID', 'Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years', 'City_Category')\
	.distinct().groupBy('Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years', 'City_Category')\
	.agg(F.collect_list('User_ID').alias('User_ID'))\
	.orderBy('Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years', 'City_Category')
demographic_distinct.show()

file = open("tmp_output_file/group_demograhic_users.ouput", 'w')
for row in demographic_distinct.collect():
	file.write(row['Gender'] + ',')
	file.write(row['Age'] + ',')
	file.write(row['Occupation'] + ',')
	file.write(row['Marital_Status'] + ',')
	file.write(row['Stay_In_Current_City_Years'] + ',')
	file.write(row['City_Category'])
	file.write(';')

	for index in range(0, len(row['User_ID'])):
		file.write(row['User_ID'][index])
		if index != len(row['User_ID']) - 1:
			file.write(',')
	file.write('\n')
file.close()

# READER
# Demographic_User = Row('Gender', 'Age', 'Occupation', 'User_ID')
# def map_group_demographic_users_file(line):
# 	gender = line.value.split(';')[0].split(',')[0]
# 	age = line.value.split(';')[0].split(',')[1]
# 	occupation = line.value.split(';')[0].split(',')[2]
# 	user_id = line.value.split(';')[1].split(',')
# 	return (gender, age, occupation, user_id)

# group_demographic_users_file = spark.read.text("tmp_output_file/group_demograhic_users.ouput").rdd
# group_demographic_users = group_demographic_users_file.map(map_group_demographic_users_file).map(lambda row: Demographic_User(*row))
# group_demographic_users = spark.createDataFrame(group_demographic_users)
# group_demographic_users.show()

# Demographic_User = Row('Gender', 'Age', 'Occupation', 'Marital_Status', 'User_ID')
# def map_group_demographic_users_file(line):
# 	gender = line.value.split(';')[0].split(',')[0]
# 	age = line.value.split(';')[0].split(',')[1]
# 	occupation = line.value.split(';')[0].split(',')[2]
# 	marital_status = line.value.split(';')[0].split(',')[3]
# 	user_id = line.value.split(';')[1].split(',')
# 	return (gender, age, occupation, marital_status, user_id)

# group_demographic_users_file = spark.read.text("tmp_output_file/group_demograhic_users.ouput").rdd
# group_demographic_users = group_demographic_users_file.map(map_group_demographic_users_file).map(lambda row: Demographic_User(*row))
# group_demographic_users = spark.createDataFrame(group_demographic_users)
# group_demographic_users.show()

# Demographic_User = Row('Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years', 'User_ID')
# def map_group_demographic_users_file(line):
# 	gender = line.value.split(';')[0].split(',')[0]
# 	age = line.value.split(';')[0].split(',')[1]
# 	occupation = line.value.split(';')[0].split(',')[2]
# 	marital_status = line.value.split(';')[0].split(',')[3]
# 	duration_of_stay = line.value.split(';')[0].split(',')[4]
# 	user_id = line.value.split(';')[1].split(',')
# 	return (gender, age, occupation, marital_status, user_id)

# group_demographic_users_file = spark.read.text("tmp_output_file/group_demograhic_users.ouput").rdd
# group_demographic_users = group_demographic_users_file.map(map_group_demographic_users_file).map(lambda row: Demographic_User(*row))
# group_demographic_users = spark.createDataFrame(group_demographic_users)
# group_demographic_users.show()

# Demographic_User = Row('Gender', 'Age', 'Occupation', 'Marital_Status', 'Stay_In_Current_City_Years', 'City_Category', 'User_ID')
# def map_group_demographic_users_file(line):
# 	gender = line.value.split(';')[0].split(',')[0]
# 	age = line.value.split(';')[0].split(',')[1]
# 	occupation = line.value.split(';')[0].split(',')[2]
# 	marital_status = line.value.split(';')[0].split(',')[3]
# 	duration_of_stay = line.value.split(';')[0].split(',')[4]
# 	city = line.value.split(';')[0].split(',')[5]
# 	user_id = line.value.split(';')[1].split(',')
# 	return (gender, age, occupation, marital_status, city, user_id)

# group_demographic_users_file = spark.read.text("tmp_output_file/group_demograhic_users.ouput").rdd
# group_demographic_users = group_demographic_users_file.map(map_group_demographic_users_file).map(lambda row: Demographic_User(*row))
# group_demographic_users = spark.createDataFrame(group_demographic_users)
# group_demographic_users.show()