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
		set_of_cates.append(cates)
	set_of_cates = list(set(set_of_cates))
	set_of_cates = [list(x) for x in set_of_cates]
	return (user_id, order_id, set_of_cates)

Order_Cates = Row('User_ID', 'Order_ID', 'Category')
group_tx_with_order_id_set_cates_file = spark.read.text("tmp_output_file/group_tx_with_order_id_set_cates.ouput").rdd
group_tx_with_order_id_set_cates = group_tx_with_order_id_set_cates_file.map(map_group_tx_with_order_id_set_cates_file)\
	.filter(lambda l: len(l) == 3).map(lambda order: Order_Cates(*order))
df = spark.createDataFrame(group_tx_with_order_id_set_cates)
df.show()

min_support = 0.01
min_confidence = 0.1

fpGrowth = FPGrowth(itemsCol="Category", minSupport=min_support, minConfidence=min_confidence)
model = fpGrowth.fit(df)

def get_total_baskets():
	return 78684

udf_get_total_baskets = udf(get_total_baskets)

# frequent_itemsets = model.freqItemsets
frequent_itemsets = spark.createDataFrame(model.freqItemsets.rdd.sortBy(lambda row: (row[1]), ascending=False))
frequent_itemsets.show()
file = open("tmp_output_file/frequent_itemsets.ouput", 'w')
for row in frequent_itemsets.collect():
	items = row['items']
	for index in range(0, len(items)):
		file.write(str(items[index]))
		if index != len(row['items']) - 1:
			file.write(';')
	file.write(':' + str(row['freq']))
	file.write('\n')
file.close()

association_rules = model.associationRules

join_df = association_rules.join(frequent_itemsets, frequent_itemsets.items == association_rules.consequent)
join_df = join_df.withColumn('interest', abs(join_df.confidence - join_df.freq / udf_get_total_baskets()))
join_df = spark.createDataFrame(join_df.rdd.sortBy(lambda row: (len(row[0]), row[5]), ascending=False))
join_df = join_df.filter(join_df.interest > 0.5)
join_df.show()

file = open("tmp_output_file/frequent_sets_cates_with_high_interest.ouput", 'w')
for row in join_df.collect():
	antecedent = row['antecedent']
	consequent = row['consequent'][0][0]
	confidence = row['confidence']
	interest = row['interest']

	for index1 in range(0, len(antecedent)):
		el_set = antecedent[index1]
		for index2 in range(0, len(el_set)):
			file.write(str(el_set[index2]))
			if index2 != len(el_set) - 1:
				file.write(',')
		if index1 != len(antecedent) - 1:
			file.write(';')
	file.write(':' + str(consequent))
	file.write(':' + str(confidence))
	file.write(':' + str(interest))
	file.write('\n')
file.close()

print("frequent items count: " + str(join_df.count()))