def compute_hashes(line):
	"""
	Compute 'broadcastHashFunctionNumber' hashes for each movie's id
	(hash function applied using different seeds)
	:param line: single line of the dataset in the format [movieId, averageRating]
	:return: array of computed hash values
	"""
	movie_id = line[0]
	bloom_filter_size = broadcastSizeOfBloomFilters[int(round(float(line[1])))][1]
	return [mmh3.hash(movie_id, i) % bloom_filter_size for i in range(broadcastHashFunctionNumber.value)]


def compute_number_of_hash_functions():
	"""
	Compute how many hash functions are required for the bloom filter
	:return: the number of hash functions
	"""
	return int(round(- math.log(false_positive_prob) / math.log(2)))


def compute_size_of_bloom_filter(number_of_inputs):
	"""
	Compute the size of a bloom filter in order to meet the required false positive probability
	:param number_of_inputs: estimated number of key that will be inserted in the bloom filter
	:return: the size of a single bloom filter
	"""
	return int(round(-(number_of_inputs * math.log(false_positive_prob)) / math.pow(math.log(2), 2)))


def get_size_of_bloom_filters():
	"""
	Retrieve the size of each Bloom Filter
	:return: an array of tuple containing the size of all Bloom Filters in the format (rating, size)
	"""
	return sc.textFile(linecount_file) \
		.map(lambda x: x.split('\t')[0:2]) \
		.map(lambda x: (x[0], compute_size_of_bloom_filter(x[1]))) \
		.collect()
