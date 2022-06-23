"""
	For each rating, count how many occurrences exist in the dataset,
	in order to correctly estimate the size of the bloom filters
"""

from pyspark import SparkContext
import argparse


def parse_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('input_file', type=str, help='path of the input file')
	parser.add_argument('output_file', type=str, help='path of the output file')
	args = parser.parse_args()
	return args.input_file, args.output_file


def main():
	# Parse command line arguments for the filename
	input_file, output_file = parse_arguments()

	# Allocate Spark context
	sc = SparkContext(appName="COUNT_NUMBER_OF_KEYS", master="yarn")

	sc.textFile(input_file) \
		.map(lambda x: x.split('\t')[0:2]) \
		.map(lambda x: (int(round(float(x[1]))), 1)) \
		.reduceByKey(lambda x, y: x + y) \
		.map(lambda x: "{0}\t{1}".format(x[0], x[1])) \
		.saveAsTextFile(output_file)


if __name__ == '__main__':
	main()
