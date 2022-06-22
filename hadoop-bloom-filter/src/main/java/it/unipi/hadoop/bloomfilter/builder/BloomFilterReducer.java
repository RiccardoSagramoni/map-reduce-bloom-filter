package it.unipi.hadoop.bloomfilter.builder;

import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Reducer of the mapreduce application that builds a bloom filter.
 * <ul>
 * <li>Input key: average rating (IntWritable)</li>
 * <li>Input value: array of position to set to 1 in the bloom filter (IntArrayWritable)</li>
 * <li>Output key: average rating (IntWritable)</li>
 * <li>Output value: bloom filter structure (BooleanArrayWritable)</li>
 * </ul>
 */
public class BloomFilterReducer
		extends Reducer<ByteWritable, IntArrayWritable, ByteWritable, BooleanArrayWritable>
{
	// Logger
	private static final Logger LOGGER = LogManager.getLogger(BloomFilterReducer.class);

	// Size of each bloom filter
	private Map<Byte, Integer> BLOOM_FILTER_SIZE;

	// Writable array for the result of the reducer (i.e. the bloom filter)
	private final BooleanArrayWritable SERIALIZABLE_BLOOM_FILTER = new BooleanArrayWritable();



	@Override
	public void setup (Context context) {
		BLOOM_FILTER_SIZE = BloomFilterUtils.readConfigurationBloomFiltersSize(context.getConfiguration());
		LOGGER.debug("Bloom filter size = " + BLOOM_FILTER_SIZE);
	}



	@Override
	public void reduce (ByteWritable key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException
	{
		LOGGER.debug("Reducer key = " + key);


		// Instantiate the temporary bloom filter, i.e. an array of BooleanWritable
		BooleanWritable[] bloomFilter = new BooleanWritable[BLOOM_FILTER_SIZE.get(key.get())];
		for (int i = 0; i < bloomFilter.length; i++) {
			bloomFilter[i] = new BooleanWritable(false);
		}
		LOGGER.debug("bloomFilter = " + Arrays.toString(bloomFilter));



		// Iterate the intermediate data
		for (IntArrayWritable array : values) {
			// Generate an iterable array
			IntWritable[] arrayWithHashedIndexes = (IntWritable[]) array.toArray();
			LOGGER.debug("IntWritable array = " + Arrays.toString(arrayWithHashedIndexes));


			// Iterate the list of BF indexes produced by mapper's hash functions
			for (IntWritable i : arrayWithHashedIndexes) {
				int indexToSet = i.get();
				LOGGER.debug("indexToSet = " + indexToSet);

				// Check if index is a valid number
				if (indexToSet < 0 || indexToSet >= bloomFilter.length) {
					LOGGER.warn("Index " + indexToSet +
							" for key " + key + " is not valid");
					continue;
				}

				// Set to true the corresponding item of the array
				if (!bloomFilter[indexToSet].get()) {
					bloomFilter[indexToSet].set(true);
				}
			}

		}

		LOGGER.debug("Bloom filter length = " + bloomFilter.length);
		LOGGER.debug("Bloom filter = " + Arrays.toString(bloomFilter));

		// Emit the reducer's results
		SERIALIZABLE_BLOOM_FILTER.set(bloomFilter);
		context.write(key, SERIALIZABLE_BLOOM_FILTER);
	}

}
