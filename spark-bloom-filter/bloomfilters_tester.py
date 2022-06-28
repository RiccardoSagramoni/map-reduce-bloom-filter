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
    parser.add_argument('false_positive_prob', type=float, help='probability of false positives')
    parser.add_argument('dataset_input_file', type=str, help='path of the dataset')
    parser.add_argument('bloom_filters_input_file', type=str, help='path to the output of the bloom filters builder')
    parser.add_argument('linecount_file', type=str, help='path of the linecount output file')
    parser.add_argument('output_file', type=str, help='path of the output file')
    args = parser.parse_args()
    
    return args.false_positive_prob, args.dataset_input_file, \
           args.bloom_filters_input_file, args.linecount_file, \
           args.output_file


def check_false_positive(indexes: list, bloom_filter: list) -> int:
    """
    Iterate the array of the hash values (i.e. the position indexes in the bloom filter)
    to check the current value.
    If at least one value is not set, then the sample is not a false positive

    :param indexes: list of indexes to check if set to True in the bloom filter
    :param bloom_filter: list of a bloom filter created in the builder

    :return: 1 or 0 if the movie to check is a false positive (1) or not (0)
    """
    
    for i in indexes:
        if not bloom_filter[i]:
            return 0

    return 1


def main():
    false_positive_prob, dataset_input_file, bloom_filters_file, linecount_file, output_file = parse_arguments()
    
    sc = SparkContext(appName="BLOOM_FILTER_TESTER", master="yarn")
    
    # Add Python dependencies to Spark application
    sc.addPyFile("bloomfilters_util.py")
    sc.addPyFile(mmh3.__file__)
    
    broadcast_hash_function_number = sc.broadcast(
        util.compute_number_of_hash_functions(false_positive_prob)
    )
    broadcast_size_of_bloom_filters = sc.broadcast(
        util.get_size_of_bloom_filters(sc, linecount_file, false_positive_prob)
    )

    print()
    print("Number of hash functions: " + str(broadcast_hash_function_number.value))
    print("Size of bloom filters: " + str(broadcast_size_of_bloom_filters.value))
    print()
    
    # Read bloom filters from the output file of the builder and distribute to the worker nodes
    bloom_filters = sc.broadcast(dict(sc.pickleFile(bloom_filters_file).collect()))
    print(bloom_filters.value)
    
    """
    TODO
        1. read dataset
        2. map: split each line to extract [movieId, averageRating]
        3. map: round averageRating to the closest integer and output the array of hashes of the movie's id
        4. reduceByKey: group by rating and merge the `indexes to set` (computed in the previous step)
                        without duplicates
        5. map: take (rating, [hashes]) and create the bloom filter setting to True the corresponding item of the array
        6. save the results (the Bloom Filter) as a pickle file
    """
    lines = sc.textFile(dataset_input_file) \
        .map(lambda line: line.split('\t')[0:2])
    
    l = lines.map(lambda x: (int(round(float(x[0]))), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .collect()
    
    broadcast_line_count = sc.broadcast(dict(l))
    
    lines.map(lambda split_line: (int(round(float(split_line[1]))),
                                  util.compute_hashes(split_line,
                                                      broadcast_size_of_bloom_filters.value,
                                                      broadcast_hash_function_number.value
                                                      )
                                  )
              ) \
        .map(lambda rating_indexes: (rating_indexes[0],
                                     check_false_positive(rating_indexes[1], bloom_filters.value[rating_indexes[0]]))
             ) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[0], (x[1], broadcast_line_count.value[x[0]], x[1] / broadcast_line_count.value[x[0]]))) \
        .saveAsTextFile(output_file)
    
    print("\n\nBLOOM FILTERS TESTER COMPLETED\n\n")


if __name__ == '__main__':
    main()
