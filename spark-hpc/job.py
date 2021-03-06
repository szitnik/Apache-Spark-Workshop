from pyspark import SparkContext, SparkConf
from random import random
from operator import add

spark_conf = SparkConf()
sc = SparkContext(appName='SparkWorkshop Python Pi', conf = spark_conf)

n = 10000000

def f(_):
  x = random() * 2 - 1
  y = random() * 2 - 1
  return 1 if x ** 2 + y ** 2 <= 1 else 0

count = sc.parallelize(range(1, n + 1), 2).map(f).reduce(add)

print("\n\n#######################")
print("Pi is roughly %f" % (4.0 * count / n))
print("#######################\n\n")

