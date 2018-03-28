import itertools
import copy

import sys
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

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

def flatmap_candidate_pairs_in_basket(basket):
	result = []

	for pair in candidate_pairs:
		if set(pair).issubset(basket['Product_ID']):
			result.append((pair, 1))

	return result

def sort_product_ids(product_ids):
	return sorted(product_ids)

udf_get_list_length = udf(get_list_length)
udf_add_order_id = udf(add_order_id)
udf_flatmap_candidate_pairs_in_basket = udf(flatmap_candidate_pairs_in_basket)
udf_sort_product_ids = udf(sort_product_ids)

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
training = spark.read.csv("dataset/train.csv", header = "true")

list_of_products = training.select('Product_ID').distinct().collect()

dict_of_products = {}
for product in list_of_products:
	dict_of_products[str(product['Product_ID'])] = 0

training = training.withColumn('Order_ID', udf_add_order_id('User_ID'))

baskets = training.select('User_ID', 'Order_ID', 'Product_ID').distinct()\
	.groupBy('User_ID', 'Order_ID').agg(F.collect_list('Product_ID').alias('Product_ID'))

baskets.withColumn('Product_ID', udf_sort_product_ids('Product_ID'))
baskets = baskets.rdd.map(lambda row: row['Product_ID']).collect()

class Node:
	def __init__(self, data):
		self.data = data
		self.count = 0
		self.childrenNodes = []
		self.childrenStr = []
		self.parent = None

	def setData(self, data):
		self.data = data
		
	def getData(self):
		return self.data

	def setNext(self, next):
		self.next = next

	def getNext(self):
		return self.next

	def getChildrenNodes(self):
		return self.childrenNodes

	def getChildrenStr(self):
		return self.childrenStr

	def addChild(self, child):
		if child.data not in self.childrenStr:
			child.parent = self
			self.childrenStr.append(child.data)
			self.childrenNodes.append(child)

	def removeChild(self, child):
		if child.data in self.childrenStr:
			child.parent = None
			self.childrenStr.remove(child.data)
			for el in self.childrenNodes:
				if el.data == child.data:
					self.childrenNodes.remove(el)

	def isAChild(self, data):
		return data in self.getChildrenStr()

	def getChildByData(self, data):
		if data in self.getChildrenStr():
			for el in self.getChildrenNodes():
				if el.data == data:
					return el
		else:
			return None

	def getParent(self):
		return self.parent

	def detachFromParent(self):
		self.parent.removeChild(self)
		self.parent = None

	def increaseCount(self):
		self.count += 1

	def getCount(self):
		return self.count

def find_frequent_itemsets_fpgrowth(baskets, min_support):
	tmp_baskets = copy.deepcopy(baskets)
	dict_frequent_single_items = {}
	frequent_single_items = []

	for basket in tmp_baskets:
		for item in basket:
			if item not in dict_frequent_single_items.keys():
				dict_frequent_single_items[item] = 0
			dict_frequent_single_items[item] += 1

	for index in range(0, len(baskets)):
		for item in baskets[index]:
			if dict_frequent_single_items[item] < min_support:
				tmp_baskets[index].remove(item)

	tmp_baskets = [x for x in tmp_baskets if len(x) > 0]

	for basket in tmp_baskets:
		tmp_list_count = []
		for item in basket:
			tmp_list_count.append(dict_frequent_single_items[item])

		tmp_list = [x for _,x in sorted(zip(tmp_list_count, basket), reverse=True)]

		for index in range(0, len(basket)):
			basket[index] = tmp_list[index]

	# print(tmp_baskets)

	# FIRST PASS
	null_node = Node(None)

	current_parent = null_node

	# this var is to simulate the linked list of items
	dict_of_items = {}

	# Construct FP-Tree
	# iterate through all the baskets to count
	for basket in tmp_baskets:
		# check and add the first item in the basket into the root
		if not current_parent.isAChild(basket[0]):
			new_node = Node(basket[0])
			current_parent.addChild(new_node)

			# create a new linked list for item ...
			if basket[0] not in dict_of_items.keys():
				dict_of_items[basket[0]] = []

			# ... and add new node into linked list
			dict_of_items[basket[0]].append(new_node)

		# loop through the remainder and add all other items into the tree
		for item in basket:
			if len(basket) == 1:
				print("basket: " + str(basket) + ", item: " + item)
			if not current_parent.isAChild(item):
				new_node = Node(item)
				current_parent.addChild(new_node)

				# create a new linked list for item ...
				if item not in dict_of_items.keys():
					dict_of_items[item] = []

				# ... and add new node into linked list
				dict_of_items[item].append(new_node)

			current_parent = current_parent.getChildByData(item)
			current_parent.increaseCount()

		current_parent = null_node

	# min_support = 2

	# # SECOND PASS
	frequent_sets = {}

	# # loop over all consequents to calculate
	# for consequent in dict_of_items:
	# 	branch_frequent_single_items = []
	# 	tmp_frequent_sets = []
	# 	branch_frequent_sets = []
	# 	branch_items_count = {}

	# 	# loop over all branches of each consequents and count individual items
	# 	for branch_leaf in dict_of_items[consequent]:
	# 		branch_count = branch_leaf.getCount()
	# 		current_node = branch_leaf.getParent()

	# 		# loop through back to the root
	# 		while current_node.getData() is not None:
	# 			if current_node.getData() not in branch_items_count.keys():
	# 				branch_items_count[current_node.getData()] = 0
	# 			branch_items_count[current_node.getData()] += branch_count

	# 			current_node = current_node.getParent()

	# 	# filter only single items that are frequent (above min support)
	# 	for item in branch_items_count:
	# 		if branch_items_count[item] >= min_support:
	# 			branch_frequent_single_items.append(item)

	# 	# append frequent single items
	# 	tmp_frequent_sets.extend(branch_frequent_single_items)

	# 	# generate combinations from set of single frequent items
	# 	for size_of_combinations in range(2, len(branch_frequent_single_items) + 1):
	# 		combinations = itertools.combinations(branch_frequent_single_items, size_of_combinations)
	# 		tmp_frequent_sets.extend(list(combinations))

	# 	for combination in tmp_frequent_sets:
	# 		tmp_list = list(combination)
	# 		tmp_list.append(consequent)
	# 		branch_frequent_sets.append(tmp_list)

	# 	# add the consequent into list
	# 	branch_frequent_sets.append(list(consequent))

	# 	frequent_sets[consequent] = branch_frequent_sets

	return frequent_sets

# baskets = [
# 	['a', 'b'],
# 	['b', 'c', 'd'],
# 	['a', 'c', 'd', 'e'],
# 	['a', 'd', 'e'],
# 	['a', 'b', 'c'],
# 	['a', 'b', 'c', 'd'],
# 	['a'],
# 	['a', 'b', 'c'],
# 	['a', 'b', 'd'],
# 	['b', 'c', 'e']
# ]
frequent_sets = find_frequent_itemsets_fpgrowth(baskets, 100)

# baskets = [
# 	['f', 'a', 'c', 'd', 'g', 'i', 'm', 'p'],
# 	['a', 'b', 'c', 'f', 'l', 'm', 'o'],
# 	['b', 'f', 'h', 'j', 'o'],
# 	['b', 'c', 'k', 's', 'p'],
# 	['a', 'f', 'c', 'e', 'l', 'p', 'm', 'n']
# ]
# frequent_sets = find_frequent_itemsets_fpgrowth(baskets, 3)

for key in frequent_sets:
	if len(frequent_sets[key]) > 0:
		print("Suffice: " + key)
		print(frequent_sets[key])
		print