"""
	Split the IMDB dataset into two partition: one is used to build the bloom filters (training),
	whereas the other is used to test the bloom filters (test)
"""

from pyspark import SparkContext
import argparse

# Approximate portion of dataset chosen for building the bloom filters
TRAINING_DATASET_PORTION = 0.6


def parse_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('filename', type=str, help='name of the input file inside the `data` folder')
	args = parser.parse_args()
	return args.filename


def main():
	print("\nSTART SPLIT_DATASET\n")
	
	# Parse command line arguments for the filename
	filename = parse_arguments()
	
	# Allocate Spark context
	sc = SparkContext(appName="SPLIT_DATASET", master="yarn")
	
	# Read dataset from HDFS
	dataset = sc.textFile("data/" + filename)
	
	# Remove header
	header = dataset.first()
	dataset = dataset.filter(lambda row: row != header)
	
	# Split dataset into train and test partition
	train, test = dataset.randomSplit([TRAINING_DATASET_PORTION, 1 - TRAINING_DATASET_PORTION])
	
	# Save results into HDFS
	train.saveAsTextFile("data/train_" + filename)
	test.saveAsTextFile("data/test_" + filename)
	
	print("\nEND SPLIT_DATASET\n")


if __name__ == '__main__':
	main()
