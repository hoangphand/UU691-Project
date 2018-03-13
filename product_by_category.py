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

udf_get_list_length = udf(get_list_length)

Product = Row('Product_ID', 'Product_Category')

training = training.rdd.flatMap(map_products_by_categories).map(lambda product: Product(*product)).distinct()

group_by_product = spark.createDataFrame(training).orderBy('Product_Category')\
	.groupBy('Product_ID').agg(F.collect_list('Product_Category').alias('Product_Category')).orderBy('Product_ID')
group_by_product.show()
print(group_by_product.count())

group_by_category = group_by_product.groupBy('Product_Category').agg(F.collect_list('Product_ID').alias('Product_ID')).orderBy('Product_Category')
group_by_category.show()
print(group_by_category.count())

group_by_category_with_count = group_by_category.withColumn('No_Of_Products', udf_get_list_length('Product_ID'))
group_by_category_with_count.show()