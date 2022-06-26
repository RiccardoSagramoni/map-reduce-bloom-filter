import math

import mmh3
from pyspark import SparkContext
import argparse


def parse_arguments():
	"""
	Parse the command line input arguments
	:return: parsed arguments
	"""
	parser = argparse.ArgumentParser()
	parser.add_argument('false_positive_prob', type=str, help='probability of false positives')
	parser.add_argument('dataset_input_file', type=str, help='path of the dataset')
	parser.add_argument('linecount_file', type=str, help='path of the linecount output file')
	parser.add_argument('output_file', type=str, help='path of the output file')
	args = parser.parse_args()

	return args.false_positive_prob, args.dataset_input_file, args.linecount_file, args.output_file


def computeHashes(line):
	"""
	Compute 'broadcastHashFunctionNumber' hashes for each movie's id
	(hash function applied using different seeds)
	:param line: single line of the dataset in the format [movieId, averageRating]
	:return: array of computed hash values
	"""
	movie_id = line[0]
	bloom_filter_size = broadcastSizeOfBloomFilters[int(round(float(line[1])))][1]
	return [mmh3.hash(movie_id, i) % bloom_filter_size for i in range(broadcastHashFunctionNumber.value)]


def computeNumberOfHashFunctions():
	"""
	Compute how many hash functions are required for the bloom filter
	:return: the number of hash functions
	"""
	return int(round(- math.log(false_positive_prob) / math.log(2)))


def computeSizeOfBloomFilter(number_of_inputs):
	"""
	Compute the size of a bloom filter in order to meet the required false positive probability
	:param number_of_inputs: estimated number of key that will be inserted in the bloom filter
	:return: the size of a single bloom filter
	"""
	return int(round(-(number_of_inputs * math.log(false_positive_prob)) / math.pow(math.log(2), 2)))


def getSizeOfBloomFilters():
	"""
	Retrieve the size of each Bloom Filter
	:return: an array of tuple containing the size of all Bloom Filters in the format (rating, size)
	"""
	return sc.textFile(linecount_file) \
		.map(lambda x: x.split('\t')[0:2]) \
		.map(lambda x: (x[0], computeSizeOfBloomFilter(x[1]))) \
		.collect()


def initializeBloomFiltersArray():
	"""
	Instantiate the temporary Bloom Filters with all values to False
	:return: the Bloom Filters created
	"""
	bloom_filters = dict()
	for i, j in broadcastSizeOfBloomFilters.value:
		bloom_filters[i] = [False for k in range(j)]
	return bloom_filters


def setBloomFilter(bloom_filter_hashes):
	"""
	Set to True the corresponding item of the Bloom Filter
	:param bloom_filter_hashes: K-V pair (rating, [hashes]), where the hash values are the index to set in the filter
	:return: the Bloom filters structure
	"""
	for i in bloom_filter_hashes[1]:
		bloomFilters[bloom_filter_hashes[0]][i] = True
	return bloomFilters


if __name__ == '__main__':
	false_positive_prob, dataset_input_file, linecount_file, output_file = parse_arguments()

	sc = SparkContext(appName="BLOOM_FILTER", master="yarn")

	broadcastHashFunctionNumber = sc.broadcast(computeNumberOfHashFunctions())
	broadcastSizeOfBloomFilters = sc.broadcast(getSizeOfBloomFilters())
	bloomFilters = initializeBloomFiltersArray()

	# map => (rating,posToSet) => x[0] = rating , x[1] = posToSet

	"""
		1. read dataset
		2. map: split each line to extract [movieId, averageRating]
		3. map: round averageRating to the closest integer and output the array of hashes of the movie's id
		4. reduceByKey: group by rating and create an unique list of all hash values computed in the previous step
		5. map: take (rating, [hashes]) and create the bloom filter setting to True the corresponding item of the array
		6. save the results (the Bloom Filter) as a text file 
	"""
	# todo remove reduceByKey stage ?
	sc.textFile(dataset_input_file) \
		.map(lambda x: x.split('\t')[0:2]) \
		.map(lambda x: (int(round(float(x[1]))), computeHashes(x))) \
		.reduceByKey(lambda x, y: list(set(x + y))) \
		.map(lambda x: (x[0], setBloomFilter(x))) \
		.saveAsTextFile(output_file)
