import sys
import pyspark
import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import abs

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.driver.memory", "16g")

def map_group_tx_with_order_id_set_cates_file(line):
	user_id = int(line.value.split(';')[0])
	order_id = int(line.value.split(';')[1])
	set_of_cates = []
	for index in range(2, len(line.value.split(';'))):
		cates = tuple([int(x) for x in line.value.split(';')[index].split(',')])
		# cates = str([int(x) for x in line.value.split(';')[index].split(',')])
		# cates = [int(x) for x in line.value.split(';')[index].split(',')]
		set_of_cates.append(cates)

	set_of_cates = list(set(set_of_cates))
	set_of_cates = [str(x) for x in set_of_cates]

	return (user_id, order_id, set_of_cates)

# def convert_array_to_tuple(line):
# 	result = []

# 	for el in line:
# 		result.append(tuple(el))

# 	return result

# udf_convert_array_to_tuple = udf(convert_array_to_tuple)

Order_Cates = Row('User_ID', 'Order_ID', 'Category')
group_tx_with_order_id_set_cates_file = spark.read.text("tmp_output_file/group_tx_with_order_id_set_cates.ouput").rdd
group_tx_with_order_id_set_cates = group_tx_with_order_id_set_cates_file.map(map_group_tx_with_order_id_set_cates_file)\
	.filter(lambda l: len(l) == 3).map(lambda order: Order_Cates(*order))
# df = group_tx_with_order_id_set_cates.toDF()
df = spark.createDataFrame(group_tx_with_order_id_set_cates)
# df = df.withColumn("Converted_Category", udf_convert_array_to_tuple('Category'))
df.show()

min_support = 0.0001
min_confidence = 0.1

fpGrowth = FPGrowth(itemsCol="Category", minSupport=min_support, minConfidence=min_confidence)
# fpGrowth = FPGrowth(itemsCol="Converted_Category", minSupport=min_support, minConfidence=min_confidence)
model = fpGrowth.fit(df)

def get_total_baskets():
	return 78684

udf_get_total_baskets = udf(get_total_baskets)

frequent_itemsets = model.freqItemsets
association_rules = model.associationRules
join_df = association_rules.join(frequent_itemsets, frequent_itemsets.items == association_rules.consequent)
join_df = join_df.withColumn('interest', abs(join_df.confidence - join_df.freq / udf_get_total_baskets()))
# join_df = spark.createDataFrame(join_df.rdd.sortBy(lambda row: (len(row[0]), row[5]), ascending=False))
join_df.show(15)

print("frequent items count: " + str(tmp_df.count()))