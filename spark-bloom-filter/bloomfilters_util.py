import math
import mmh3
from pyspark import SparkContext


def compute_hashes(line: str, size_of_bloom_filters: dict, hash_function_number: int) -> list:
    """
    Compute the hash values of a movie (MurmurHash functions applied using different seeds)
    
    :param line: single line of the dataset in the format [movieId, averageRating]
    :param size_of_bloom_filters: dict with the size of each bloom filter
    :param hash_function_number: how many hash functions must be used
    
    :return: array of computed hash values
    """
    # Parse line
    movie_id = line[0]
    rating = int(round(float(line[1])))
    # Get bloom filter size
    bloom_filter_size = size_of_bloom_filters[rating]
    # Compute hash values
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
