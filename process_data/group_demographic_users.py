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

# demographic_distinct = training.select('User_ID', 'Gender', 'Age', 'Occupation', \
# 	'Marital_Status').distinct().groupBy('Gender', 'Age', 'Occupation', 'Marital_Status')\
# 	.agg(F.collect_list('User_ID').alias('User_ID')).orderBy('Gender', 'Age', 'Occupation', 'Marital_Status')
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

# READER
Demographic_User = Row('Gender', 'Age', 'Occupation', 'User_ID')
def map_group_demographic_users_file(line):
	gender = line.value.split(';')[0].split(',')[0]
	age = line.value.split(';')[0].split(',')[1]
	occupation = line.value.split(';')[0].split(',')[2]
	user_id = line.value.split(';')[1].split(',')
	return (gender, age, occupation, user_id)

group_demographic_users_file = spark.read.text("tmp_output_file/group_demograhic_users.ouput").rdd
group_demographic_users = group_demographic_users_file.map(map_group_demographic_users_file).map(lambda row: Demographic_User(*row))
group_demographic_users = spark.createDataFrame(group_demographic_users)
group_demographic_users.show()