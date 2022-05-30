from pyspark import SparkContext

if __name__ == '__main__':
	sc = SparkContext(appName="ESTIMATE NUMBER OF KEYS", master="yarn")
