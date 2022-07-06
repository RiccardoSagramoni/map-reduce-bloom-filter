package it.unipi.hadoop.bloomfilter.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class BloomFilterUtils {
	// Logger
	private static final Logger LOGGER = LogManager.getLogger(BloomFilterUtils.class);



	/**
	 * Generate the configuration for the bloom filter building process.
	 * @param configuration configuration of MapReduce job
	 * @param falsePositiveProbability required false positive probability
	 * @param sizeOfBloomFilters map with the size of the corresponding bloom filter for each rating value
	 */
	public static void generateConfiguration(Configuration configuration,
	                                         double falsePositiveProbability,
	                                         Map<Byte, Integer> sizeOfBloomFilters)
	{
		// Set bloomFilter parameters
		configuration.setInt(
				BloomFilterConfigurationName.NUMBER.toString(),	// how many bloom filter must be created
				sizeOfBloomFilters.size()
		);

		int i = 0;
		for (Map.Entry<Byte, Integer> entry : sizeOfBloomFilters.entrySet()) {
			configuration.setInt(
					BloomFilterConfigurationName.RATING_KEY.toString() + i, // rating key of the k-th bloom filter
					entry.getKey()
			);
			configuration.setInt(
					BloomFilterConfigurationName.SIZE_VALUE.toString() + i, // size of k-th bloom filter
					entry.getValue()
			);
			i++;
		}

		configuration.setInt(
				BloomFilterConfigurationName.NUMBER_HASH.toString(),	// how many hash functions for each bloom filter
				computeNumberOfHashFunctions(falsePositiveProbability)
		);

		System.out.println("Number of hash functions: " + computeNumberOfHashFunctions(falsePositiveProbability));
		System.out.println("Rating -> bloom filter size: " + sizeOfBloomFilters);
	}



	/**
	 * Read the configuration for each bloom filter and create the key-value pairs (rating, size)
	 * @param configuration configuration of MapReduce job
	 * @return map structure with the size of the corresponding bloom filter for each rating value
	 */
	public static Map<Byte, Integer> readConfigurationBloomFiltersSize (Configuration configuration) {
		// Structure that maps each rating value to the size of the corresponding bloom filter
		Map<Byte, Integer> map = new HashMap<>();

		// Read how many bloom filters will be created
		int howManyBloomFilters = configuration.getInt(BloomFilterConfigurationName.NUMBER.toString(), -1);
		if (howManyBloomFilters <= 0) {
			LOGGER.error(BloomFilterConfigurationName.NUMBER +
					" parameter not set");
			throw new RuntimeException(BloomFilterConfigurationName.NUMBER +
					" parameter not set");
		}

		// For each bloom filter, extract the related key and the size of the bloom filter.
		// Build a hash map with the configuration parameters.
		for (int i = 0; i < howManyBloomFilters; i++) {
			byte key = (byte) configuration.getInt(
					BloomFilterConfigurationName.RATING_KEY.toString() + i,
					-1
			);
			int size = configuration.getInt(
					BloomFilterConfigurationName.SIZE_VALUE.toString() + i,
					-1
			);

			if (key < 0 || size <= 0) {
				LOGGER.warn("Configuration isn't valid: " +
						BloomFilterConfigurationName.RATING_KEY + i + " = " + key + ", " +
						BloomFilterConfigurationName.SIZE_VALUE + i
				);
				continue;
			}

			map.put(key, size);
		}

		return map;
	}



	/**
	 * Compute how many hash functions are required for the bloom filter
	 * @param falsePositiveProbability desired probability of a false positive
	 * @return the number of hash functions
	 */
	public static int computeNumberOfHashFunctions(double falsePositiveProbability) {
		return (int) Math.ceil(
						- Math.log(falsePositiveProbability)
						/
						Math.log(2)
		);
	}



	/**
	 * Compute the size of a bloom filter in order to meet the required false positive probability
	 * @param falsePositiveProbability desired probability of a false positive
	 * @param numberOfInputs estimated number of key that will be inserted in the bloom filter
	 * @return the size of the bloom filter
	 */
	public static int computeSizeOfBloomFilter(double falsePositiveProbability, int numberOfInputs) {
		return (int) Math.ceil(
						- ( numberOfInputs * Math.log(falsePositiveProbability) )
						/
						Math.pow(Math.log(2), 2)
		);
	}



	/**
	 * Read from file the number of input key for each bloom filter and compute the
	 * required size of the data structure.
	 * @param configuration job configuration
	 * @param file path to the file generated from the `LineCount` MapReduce application
	 * @param falsePositiveProbability desired false positive probability
	 * @return map with the size of the corresponding bloom filter for each rating value
	 */
	public static Map<Byte, Integer> getBloomFiltersSizeParameters (Configuration configuration,
	                                                                Path file,
	                                                                Double falsePositiveProbability)
			throws IOException
	{
		// Map each rating to the size its corresponding bloom filter
		Map<Byte, Integer> sizeOfBloomFilters = new HashMap<>();

		// Open file in HDFS
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(
						file.getFileSystem(configuration).open(file)
				)
			)
		){
			String line;

			// Read each record of the dataset
			while ((line = reader.readLine()) != null) {
				// Split the record
				String[] splits = line.split("\\s+");

				// Parse the record and put it in the map
				sizeOfBloomFilters.put(
						Byte.parseByte(splits[0]),
						computeSizeOfBloomFilter(falsePositiveProbability, Integer.parseInt(splits[1]))
				);
			}

		}

		return sizeOfBloomFilters;
	}

}
