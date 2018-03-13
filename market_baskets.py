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

def get_list_length(list):
	return len(list)

global_order_count = 0
last_user_id = -1

def add_order_id(current_user_id):
	global last_user_id
	global global_order_count
	if last_user_id == -1:
		last_user_id = current_user_id
		global_order_count = 1
		return global_order_count

	if current_user_id != last_user_id:
		last_user_id = current_user_id
		global_order_count = global_order_count + 1

	return global_order_count

udf_get_list_length = udf(get_list_length)
udf_add_order_id = udf(add_order_id)

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

training = training.withColumn('Order_ID', udf_add_order_id('User_ID'))

training.show()

basket_without_quantity = training.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID')).orderBy('User_ID', 'Order_ID')
basket_without_quantity.show()

basket_with_quantity = basket_without_quantity.withColumn('Quantity', udf_get_list_length('Product_ID')).orderBy('User_ID', 'Order_ID')
basket_with_quantity.show()

print(basket_with_quantity.count())