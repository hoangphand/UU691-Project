import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

def map_products_by_categories(product):
	arr = []
	if product['Product_Category_1'] != None:
		arr.append((product['Product_ID'], int(product['Product_Category_1'])))
	
	if product['Product_Category_2'] != None:
		arr.append((product['Product_ID'], int(product['Product_Category_2'])))

	if product['Product_Category_3'] != None:
		arr.append((product['Product_ID'], int(product['Product_Category_3'])))

	return arr

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

udf_add_order_id = udf(add_order_id)

udf_get_list_length = udf(get_list_length)

# Product = Row('Product_ID', 'Product_Category')

# training = training.rdd.flatMap(map_products_by_categories).map(lambda product: Product(*product)).distinct()

# group_by_product = spark.createDataFrame(training).orderBy('Product_Category')\
# 	.groupBy('Product_ID').agg(F.collect_list('Product_Category').alias('Product_Category')).orderBy('Product_ID')
# # group_by_product.show()
# # print(group_by_product.count())

# group_by_category = group_by_product.groupBy('Product_Category').agg(F.collect_list('Product_ID')\
# 	.alias('Product_ID')).orderBy('Product_Category')
# # group_by_category.show()
# # print(group_by_category.count())

# # group_by_category_with_count = group_by_category.withColumn('No_Of_Products', udf_get_list_length('Product_ID'))
# # group_by_category_with_count.show()

# group_by_product_dict = group_by_product.rdd.map(lambda product: (product['Product_ID'], product['Product_Category']))\
# 	.collectAsMap()
# group_by_category_dict = group_by_category.rdd.map(lambda cate: (cate['Product_Category'], 0)).collectAsMap()

# def map_user_product_purchased(tx):
# 	pass

dataset_with_order_id = training.withColumn('Order_ID', udf_add_order_id('User_ID'))

basket_without_quantity = dataset_with_order_id.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID'))\
	.orderBy('User_ID', 'Order_ID')

basket_without_quantity.show()