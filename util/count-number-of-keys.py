"""
	For each rating, count how many occurrences exist in the dataset,
	in order to correctly estimate the size of the bloom filters
"""
import argparse
from pyspark import SparkContext
from typing import Tuple


def parse_arguments() -> Tuple[str, str]:
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
	
	"""
		1. read dataset
		2. map: split each line to extract the average rating value
		3. map: round averageRating to the closest integer and output (rating, 1)
		4. reduceByKey: group by rating and count how many lines exist for each key (= sum the 1s)
		5. map: generate output. Take (rating, count) and output string "rating count"
		6. save the results as a text file
	"""
	sc.textFile(input_file) \
		.map(lambda line: line.split('\t')[1]) \
		.map(lambda rating: (round(float(rating)), 1)) \
		.reduceByKey(lambda value_1, value_2: value_1 + value_2) \
		.map(lambda rating_count_pair: "{0}\t{1}".format(rating_count_pair[0], rating_count_pair[1])) \
		.saveAsTextFile(output_file)


if __name__ == '__main__':
	main()
