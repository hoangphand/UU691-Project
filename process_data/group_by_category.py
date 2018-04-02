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
Product = Row('Product_ID', 'Product_Category')
Cate_Product = Row('Product_Category', 'Product_ID')

# READ products with sale
def map_products_with_sale_file(line):
	return (line.value.split(':')[0], line.value.split(':')[1])

products_with_sale_file = spark.read.text("tmp_output_file/products_with_sale.output").rdd
products_with_sale = products_with_sale_file.map(map_products_with_sale_file)
products_with_sale_dict = products_with_sale.collectAsMap()

# READ group by product
def map_group_by_product_file(line):
	product_cate = line.value.split(';')[1].split(',')
	return (line.value.split(';')[0], product_cate)

group_by_product_file = spark.read.text("tmp_output_file/group_by_product.ouput").rdd
group_by_product = group_by_product_file.map(map_group_by_product_file).map(lambda product: Product(*product))
group_by_product = spark.createDataFrame(group_by_product)
group_by_product.show()

group_by_category = group_by_product.groupBy('Product_Category').agg(F.collect_list('Product_ID')\
	.alias('Product_ID')).orderBy('Product_Category')

# REARRANGE products
def rearrange_products(row):
	array = row['Product_ID']
	array_count = []

	for el in row['Product_ID']:
		array_count.append(int(products_with_sale_dict[el]))

	sorted_products = [x for _, x in sorted(zip(array_count, array), reverse=True)]
	
	return (row['Product_Category'], sorted_products)

group_by_category = group_by_category.rdd.map(rearrange_products).map(lambda row: Cate_Product(*row))
group_by_category = spark.createDataFrame(group_by_category)
# group_by_category.show()
file = open("tmp_output_file/group_by_category.ouput", 'w')
for row in group_by_category.collect():
	for index in range(0, len(row['Product_Category'])):
		file.write(str(row['Product_Category'][index]))
		if index != len(row['Product_Category']) - 1:
			file.write(',')

	file.write(';')

	for index in range(0, len(row['Product_ID'])):
		file.write(str(row['Product_ID'][index]))
		if index != len(row['Product_ID']) - 1:
			file.write(',')
	file.write('\n')
file.close()

# READER
# Cate_Product = Row('Product_Category', 'Product_ID')
# def map_group_by_category_file(line):
# 	cate = [int(i) for i in line.value.split(';')[0].split(',')]
# 	product = line.value.split(';')[1].split(',')
# 	return (cate, product)

# group_by_category_file = spark.read.text("tmp_output_file/group_by_category.ouput").rdd
# group_by_category = group_by_category_file.map(map_group_by_category_file).map(lambda cate: Cate_Product(*cate))
# group_by_category = spark.createDataFrame(group_by_category)
# group_by_category.show()