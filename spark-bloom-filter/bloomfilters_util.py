import math
import mmh3
from pyspark import SparkContext
from typing import Tuple


def compute_number_of_hash_functions(false_positive_prob: float) -> int:
    """
    Compute how many hash functions are required for the bloom filter
    
    :param false_positive_prob: desired probability of a false positive
    
    :return: the number of hash functions
    """
    return math.ceil(- math.log(false_positive_prob) / math.log(2))


def compute_size_of_bloom_filter(false_positive_prob: float, number_of_inputs: int) -> int:
    """
    Compute the size of a bloom filter in order to meet the required false positive probability
    
    :param false_positive_prob: desired probability of a false positive
    :param number_of_inputs: estimated number of key that will be inserted in the bloom filter
    
    :return: the size of a single bloom filter
    """
    return math.ceil(-(number_of_inputs * math.log(false_positive_prob)) / math.pow(math.log(2), 2))


def get_size_of_bloom_filters(sc: SparkContext, linecount_file: str, false_positive_prob: float) -> dict:
    """
    Get the size of each bloom filter
    
    :param sc: Spark context
    :param linecount_file: name of the file with the number of keys for each rating
    :param false_positive_prob: desired probability of a false positive
    
    :return: a dictionary containing the size of all bloom filters with rating as key
    and number of occurrences as value
    """
    
    '''
        MapReduce job:
        1. Read from file text
        2. map: split each line to extract the tuple (rating, number of occurrences)
        3. map: generate a tuple (rating, size of bloom filter)
        4. collect: return the tuples as a list
    '''
    size_of_bf_list = sc.textFile(linecount_file) \
        .map(lambda line: line.split('\t')[0:2]) \
        .map(lambda split_line: (int(split_line[0]),
                                 compute_size_of_bloom_filter(false_positive_prob, int(split_line[1])))
             ) \
        .collect()
    
    # Convert the list of tuples into a dictionary
    return dict(size_of_bf_list)


def compute_indexes_to_set(movie_id: str, rating: int, size_of_bloom_filters: dict, hash_function_number: int) -> list:
    """
    Get the indexes to set in a bloom filter by computing hash values of a movie (MurmurHash functions applied using
    different seeds)

    :param movie_id: id of the movie
    :param rating: rating of the movie (= key of the bloom filter)
    :param size_of_bloom_filters: dict with the size of each bloom filter
    :param hash_function_number: how many hash functions must be used

    :return: array with the indexes to set
    """
    # Get bloom filter size
    bloom_filter_size = size_of_bloom_filters[rating]
    # Compute hash values
    return [mmh3.hash(movie_id, i) % bloom_filter_size for i in range(hash_function_number)]


def create_pair_rating_indexes(line: str, size_of_bloom_filters: dict, hash_function_number: int) -> Tuple[int, list]:
    """
    Given a line from the dataset, it does the following actions:
        - Extract the `movie id` and `rating` from the line
        - Compute the hash values of `movie id`
        - Compute the modulo of the hash values in order to identify the indexes to set in the bloom filter
        - Return a tuple with rating (which is the key of the bloom filter) and the indexes to set
    
    :param line: line extracted from the database
    :param size_of_bloom_filters: dictionary with the size of the bloom filter (value) for each rating value (key)
    :param hash_function_number: how many hash functions must be used
    :return:
    """
    # Split line and parse arguments
    split_line = line.split('\t')[0:2]
    movie_id = split_line[0]
    rating = int(float(split_line[1]) + 0.5)  # round() is bugged in Python 3.6!!!
    
    return (
        rating,
        compute_indexes_to_set(movie_id,
                               rating,
                               size_of_bloom_filters,
                               hash_function_number)
    )
