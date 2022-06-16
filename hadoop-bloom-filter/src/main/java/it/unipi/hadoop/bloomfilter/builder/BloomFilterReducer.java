package it.unipi.hadoop.bloomfilter.builder;

import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer of the mapreduce application that builds a bloom filter.
 * <ul>
 * <li>Input key: average rating (IntWritable)</li>
 * <li>Input value: array of position to set to 1 in the bloom filter (IntArrayWritable)</li>
 * <li>Output key: average rating (IntWritable)</li>
 * <li>Output value: bloom filter structure (BooleanArrayWritable)</li>
 * </ul>
 */
public class BloomFilterReducer extends Reducer<ByteWritable, IntArrayWritable, ByteWritable, BooleanArrayWritable>  {

	// Size of the bloom filter (taken from mapreduce configuration)
	private int[] BLOOM_FILTERS_SIZE;
	// Writable array for the result of the reducer (i.e. the bloom filter)
	private final BooleanArrayWritable SERIALIZABLE_BLOOM_FILTER = new BooleanArrayWritable();

	@Override
	public void setup (Context context) {
		// Retrieve configuration
		Configuration configuration = context.getConfiguration();

		// Read how many bloom filters will be created and allocate the array for the sizes
		int howManyBloomFilters = configuration.getInt("bloom.filter.number", -1);
		if (howManyBloomFilters <= 0) {
			System.err.println("[MAPPER]: bloom.filter.number parameter not set");
			return;
		}
		BLOOM_FILTERS_SIZE = new int[howManyBloomFilters];

		// Read how large each bloom filter must be
		// and build the array BLOOM_FILTERS_SIZE
		for (int i = 0; i < howManyBloomFilters; i++) {
			int bloomFilterSize = configuration.getInt(
					"bloom.filter.size." + i,
					-1
			);
			if (bloomFilterSize <= 0) {
				System.err.println("[MAPPER]: bloom.filter.hash." + i + " parameter not set");
				return;
			}
			BLOOM_FILTERS_SIZE[i] = bloomFilterSize;
		}

	}

	@Override
	public void reduce (ByteWritable key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException
	{
		int bloomFilterSize = BLOOM_FILTERS_SIZE[key.get()];

		// Instantiate the temporary bloom filter, i.e. an array of BooleanWritable
		BooleanWritable[] bloomFilter = new BooleanWritable[bloomFilterSize];

		// Iterate the intermediate data
		for (IntArrayWritable array : values) {
			// Generate an iterable array
			IntWritable[] arrayWithHashedIndexes = (IntWritable[]) array.toArray();

			// Iterate the list of BF indexes produced by mapper's hash functions
			for (IntWritable i : arrayWithHashedIndexes) {
				int indexToSet = i.get();

				// Check if index is a valid number
				if (indexToSet >= bloomFilterSize) {
					System.err.println("[REDUCER]: index " + indexToSet +
							" for key " + key + " is not valid");
					continue;
				}

				// Set to true the corresponding item of the array
				if (!bloomFilter[indexToSet].get()) {
					bloomFilter[indexToSet].set(true);
				}
			}
		}

		// Emit the reducer's results
		SERIALIZABLE_BLOOM_FILTER.set(bloomFilter);
		context.write(key, SERIALIZABLE_BLOOM_FILTER);
	}
}
