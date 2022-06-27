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


def generate_bloom_filter(indexes_to_set: list, size: int) -> list:
    """
    Set to True the corresponding item of the Bloom Filter
    
    :param indexes_to_set: list of indexes to set True (computed through hash functions)
    :param size: size of the bloom filter
    
    :return: the Bloom filters structure
    """
    bloom_filter = [False for _ in range(size)]
    
    for i in indexes_to_set:
        bloom_filter[i] = True
    
    return bloom_filter


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
    
    # map => (rating,posToSet) => x[0] = rating , x[1] = posToSet
    
    """
        1. read dataset
        2. map: split each line to extract [movieId, averageRating]
        3. map: round averageRating to the closest integer and output the array of hashes of the movie's id
        4. reduceByKey: group by rating and create an unique list of all hash values computed in the previous step
        5. map: take (rating, [hashes]) and create the bloom filter setting to True the corresponding item of the array
        6. save the results (the Bloom Filter) as a pickle file
    """
    # todo remove reduceByKey stage ?
    sc.textFile(dataset_input_file) \
        .map(lambda line: line.split('\t')[0:2]) \
        .map(lambda split_line: (int(round(float(split_line[1]))),
                                 util.compute_hashes(split_line,
                                                     broadcast_size_of_bloom_filters.value,
                                                     broadcast_hash_function_number.value
                                                     )
                                 )
             ) \
        .reduceByKey(lambda index_list_1, index_list_2: index_list_1 + index_list_2) \
        .map(lambda pair_rating_hashes:
             (pair_rating_hashes[0],
              generate_bloom_filter(pair_rating_hashes[1],
                                    broadcast_size_of_bloom_filters.value[pair_rating_hashes[0]]
                                    )
              )
             ) \
        .saveAsPickleFile(output_file)


if __name__ == '__main__':
    main()
