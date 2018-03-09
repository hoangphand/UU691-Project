import sys
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import grouping
from pyspark.sql.functions import mean
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("demo-cluster").getOrCreate()
data = spark.read.csv("dataset/train.csv", header = "true")

(training, test) = ratings.randomSplit([0.8, 0.2], seed=1234)

# preprocess data
# first approach: convert groups of age to 

data.show()