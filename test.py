from pyspark import SparkContext, SparkConf

spark_conf = SparkConf()
#spark_conf.set('spark.driver.host', '10.0.2.99')

sc = SparkContext(appName='Test', conf = spark_conf)

txt = sc.textFile('./test.txt')
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print("*******************************************")
print("LINES COUNT is: " + str(python_lines.count()))
print("*******************************************")
