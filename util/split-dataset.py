"""
	Split the IMDB dataset into two partition: one is used to build the bloom filters (training),
	whereas the other is used to test the bloom filters (test)
"""

from pyspark import SparkContext

# Approximate portion of dataset chosen for building the bloom filters
TRAINING_DATASET_PORTION = 0.6

if __name__ == '__main__':
	print("\nSTART SPLIT_DATASET\n")

	sc = SparkContext(appName="SPLIT_DATASET", master="yarn")

	# Read dataset from HDFS
	dataset = sc.textFile("data/imdb.tsv")

	# Remove header
	header = dataset.first()
	dataset = dataset.filter(lambda row: row != header)

	# Split dataset into train and test partition
	train, test = dataset.randomSplit([TRAINING_DATASET_PORTION, 1 - TRAINING_DATASET_PORTION])

	# Save results into HDFS
	train.saveAsTextFile("data/train_imdb.tsv")
	test.saveAsTextFile("data/test_imdb.tsv")

	print("\nEND SPLIT_DATASET\n")
