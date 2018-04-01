import sys
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc

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

def get_list_list_length(list):
	sum = 0
	for el in list:
		sum += len(el)
	return sum

def map_construct_user_product_matrix(user):
	tmp_matrix = dict(group_by_category_dict)
	tmp_matrix['User_ID'] = user['User_ID']

	for basket in user['Product_ID']:
		for product in basket:
			product_cate = tuple(group_by_product_dict[product])
			tmp_matrix[product_cate] += 1

	return tmp_matrix

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
udf_get_list_list_length = udf(get_list_list_length)

Product = Row('Product_ID', 'Product_Category')
def map_group_by_product_file(line):
	product_cate = line.value.split(';')[1].split(',')
	return (line.value.split(';')[0], product_cate)

group_by_product_file = spark.read.text("tmp_output_file/group_by_product.ouput").rdd
group_by_product = group_by_product_file.map(map_group_by_product_file).map(lambda product: Product(*product))
group_by_product = spark.createDataFrame(group_by_product)

Cate_Product = Row('Product_Category', 'Product_ID')
def map_group_by_category_file(line):
	cate = line.value.split(';')[0].split(',')
	product = line.value.split(';')[1].split(',')
	return (cate, product)

group_by_category_file = spark.read.text("tmp_output_file/group_by_category.ouput").rdd
group_by_category = group_by_category_file.map(map_group_by_category_file).map(lambda cate: Cate_Product(*cate))
group_by_category = spark.createDataFrame(group_by_category)

group_by_product_dict = group_by_product.rdd.map(lambda product: (product['Product_ID'], product['Product_Category']))\
	.collectAsMap()
group_by_category_dict = group_by_category.rdd.map(lambda cate: (tuple(cate['Product_Category']), 0)).collectAsMap()
# print(group_by_category_dict)

# dataset_with_order_id = training.withColumn('Order_ID', udf_add_order_id('User_ID'))

# user_order = dataset_with_order_id.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
# 	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID'))\
# 	.orderBy('User_ID', 'Order_ID')
# user_order_with_count = user_order.withColumn('Quantity', udf_get_list_length('Product_ID')).orderBy('User_ID')

# user_order_with_count.show()

# user_products_without_order_id = user_order.select('User_ID', 'Product_ID')\
# 	.groupBy('User_ID').agg(F.collect_list('Product_ID').alias('Product_ID'))\
# 	.withColumn('Quantity', udf_get_list_list_length('Product_ID')).orderBy('User_ID', desc('Quantity'))
# user_products_without_order_id.show()

# user_product_matrix_rdd = user_products_without_order_id.rdd.map(map_construct_user_product_matrix)
# user_product_matrix = user_product_matrix_rdd.collect()

# file = open("tmp_output_file/user_product_matrix_237.ouput", 'w')
# for user in user_product_matrix:
# 	file.write(str(user) + '\n')
# 	# print(user_product_matrix)
# file.close()