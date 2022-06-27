import math
import mmh3
from pyspark import SparkContext


def compute_hashes(line: str, size_of_bloom_filters: list, hash_function_number: int) -> list:
    """
    Compute the hash values of a movie (MurmurHash functions applied using different seeds)
    
    :param line: single line of the dataset in the format [movieId, averageRating]
    :param size_of_bloom_filters: array with the size of each bloom filter
    :param hash_function_number: how many hash functions must be used
    
    :return: array of computed hash values
    """
    movie_id = line[0]
    bloom_filter_size = size_of_bloom_filters[int(round(float(line[1])))][1]
    return [mmh3.hash(movie_id, i) % bloom_filter_size for i in range(hash_function_number)]


def compute_number_of_hash_functions(false_positive_prob: float) -> int:
    """
    Compute how many hash functions are required for the bloom filter
    
    :param false_positive_prob: desired probability of a false positive
    
    :return: the number of hash functions
    """
    return int(round(- math.log(false_positive_prob) / math.log(2)))


def compute_size_of_bloom_filter(false_positive_prob: float, number_of_inputs: int) -> int:
    """
    Compute the size of a bloom filter in order to meet the required false positive probability
    
    :param false_positive_prob: desired probability of a false positive
    :param number_of_inputs: estimated number of key that will be inserted in the bloom filter
    
    :return: the size of a single bloom filter
    """
    return int(round(-(number_of_inputs * math.log(false_positive_prob)) / math.pow(math.log(2), 2)))


def get_size_of_bloom_filters(sc: SparkContext, linecount_file: str, false_positive_prob: float) -> list:
    """
    Retrieve the size of each Bloom Filter
    
    :param sc: Spark context
    :param linecount_file: name of the file with the number of keys for each rating
    :param false_positive_prob: desired probability of a false positive
    
    :return: an array of tuple containing the size of all Bloom Filters in the format (rating, size)
    """
    return sc.textFile(linecount_file) \
        .map(lambda x: x.split('\t')[0:2]) \
        .map(lambda x: (x[0], compute_size_of_bloom_filter(false_positive_prob, int(x[1])))) \
        .collect()
