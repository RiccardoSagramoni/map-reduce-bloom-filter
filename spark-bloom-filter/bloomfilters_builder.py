import argparse
import bloomfilters_util as util
import mmh3
from pyspark import SparkContext


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


def initialize_bloom_filters_array(size_of_bloom_filters: list) -> dict:
    """
    Instantiate the temporary Bloom Filters with all values to False
    
    :param size_of_bloom_filters: size of the bloom filters
    
    :return: the Bloom Filters created
    """
    bloom_filters = dict()
    for i, j in size_of_bloom_filters:
        bloom_filters[i] = [False for _ in range(j)]
    return bloom_filters


def set_bloom_filter(bloom_filters: dict, bloom_filter_hashes: list) -> dict:
    """
    Set to True the corresponding item of the Bloom Filter
    
    :param bloom_filters: the bloom filters to set
    :param bloom_filter_hashes: K-V pair (rating, [hashes]), where the hash values are the index to set in the filter
    
    :return: the Bloom filters structure
    """
    for i in bloom_filter_hashes[1]:
        bloom_filters[bloom_filter_hashes[0]][i] = True
    return bloom_filters


def main():
    false_positive_prob, dataset_input_file, linecount_file, output_file = parse_arguments()
    
    sc = SparkContext(appName="BLOOM_FILTER", master="yarn")
    
    # Add Python dependencies to Spark application
    sc.addPyFile("bloomfilters_util.py")
    sc.addPyFile(mmh3.__file__)
    
    broadcast_hash_function_number = sc.broadcast(util.compute_number_of_hash_functions(false_positive_prob))
    broadcast_size_of_bloom_filters = sc.broadcast(
        util.get_size_of_bloom_filters(sc, linecount_file, false_positive_prob)
    )
    # todo: bloom filter non dovrebbe essere broadcast?
    bloom_filters = initialize_bloom_filters_array(broadcast_size_of_bloom_filters.value)
    
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
        .map(lambda x: (int(round(float(x[1]))),
                        util.compute_hashes(x,
                                            broadcast_size_of_bloom_filters.value,
                                            broadcast_hash_function_number.value
                                            )
                        )
             ) \
        .reduceByKey(lambda x, y: list(set(x + y))) \
        .map(lambda x: (x[0], set_bloom_filter(bloom_filters, x))) \
        .saveAsPickleFile(output_file)


if __name__ == '__main__':
    main()
