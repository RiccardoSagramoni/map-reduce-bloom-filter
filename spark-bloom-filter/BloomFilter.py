import math

import mmh3
from pyspark import SparkContext
import argparse


def parse_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('false_positive_prob', type=str, help='probability of false positives')
	parser.add_argument('dataset_input_file', type=str, help='path of the dataset')
	parser.add_argument('linecount_file', type=str, help='path of the linecount output file')
	parser.add_argument('output_file', type=str, help='path of the output file')
	args = parser.parse_args()
	return args.false_positive_prob, args.dataset_input_file, args.linecount_file, args.output_file


def computeHash(id, hashFunctionNumber, bloomFilterSize):
	hashValues = []
	for i in range(hashFunctionNumber):
		hashValues.extend(mmh3.hash(id, i) % bloomFilterSize)

	return hashValues


def computeNumberOfHashFunctions(false_positive_prob):
	return int(round(-math.log(false_positive_prob) / math.log(2)))


def computeBloomFiltersSize(sc, linecount_file, false_positive_prob):
	return sc.textFile(linecount_file) \
		.map(lambda x: x.split('\t')[0:2]) \
		.map(lambda x: (x[0], int(round((-x[1]*math.log(false_positive_prob))/(math.pow(math.log(2), 2))))))


def initializeBloomFiltersArray(bloomFilterSize):
	bloomFilters = dict()
	for i, j in bloomFilterSize:
		bloomFilters[i] = [False for k in range(j)]
	return bloomFilters


def setBloomFilter(bloomFilter, indexToSet):
	for i in indexToSet:
		bloomFilter[i] = True
	return bloomFilter


# todo add broadcast variables for linecount --> (rating, linecount) (?)
def main():
	false_positive_prob, dataset_input_file, linecount_file, output_file = parse_arguments()

	sc = SparkContext(appName="BLOOM_FILTER", master="yarn")

	hashFunctionNumber = sc.broadcast(computeNumberOfHashFunctions(false_positive_prob))
	bloomFilterSize = sc.broadcast(computeBloomFiltersSize(sc, linecount_file, false_positive_prob))
	bloomFilters = initializeBloomFiltersArray(bloomFilterSize)

# map => (rating,posToSet) => x[0] = rating , x[1] = posToSet

	sc.textFile(dataset_input_file) \
		.map(lambda x: x.split('\t')[0:2]) \
		.map(lambda x: (int(round(float(x[1]))), computeHash(x[0], hashFunctionNumber.value, bloomFilterSize.value[int(round(float(x[1])))][1]))) \
		.reduceByKey(lambda x: setBloomFilter(bloomFilters[x[0]], x[1])) \
		.saveAsTextFile(output_file)


if __name__ == '__main__':
	main()
