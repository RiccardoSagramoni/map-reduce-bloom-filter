package it.unipi.hadoop.bloomfilter.builder;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class BloomFilterUtils {

	private static final Logger LOGGER = LogManager.getLogger(BloomFilterUtils.class);

	/**
	 * TODO
	 * @param configuration
	 * @return
	 */
	public static Map<Byte, Integer> readConfigurationBloomFiltersSize (Configuration configuration) {
		// Structure that maps each rating value to the size of the corresponding bloom filter
		Map<Byte, Integer> map = new HashMap<>();

		// Read how many bloom filters will be created
		int howManyBloomFilters = configuration.getInt("bloom.filter.number", -1);
		if (howManyBloomFilters <= 0) {
			LOGGER.error("bloom.filter.number parameter not set");
			throw new RuntimeException("bloom.filter.number parameter not set");
		}

		// For each bloom filter, extract the related key and the size of the bloom filter.
		// Build a hash map with the configuration parameters.
		for (int i = 0; i < howManyBloomFilters; i++) {
			byte key = (byte) configuration.getInt(
					"bloom.filter.size.key." + i,
					-1
			);
			int size = configuration.getInt(
					"bloom.filter.size.value." + i,
					-1
			);

			if (key < 0 || size <= 0) {
				LOGGER.warn("Configuration isn't valid: " +
						"bloom.filter.size.key." + i + " = " + key + ", " +
						"bloom.filter.size.value." + i
				);
				continue;
			}

			map.put(key, size);
		}

		return map;
	}

}
