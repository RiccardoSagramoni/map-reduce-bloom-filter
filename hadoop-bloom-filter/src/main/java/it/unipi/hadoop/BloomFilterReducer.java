package it.unipi.hadoop;

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
public class BloomFilterReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, BooleanArrayWritable>  {

	// Size of the bloom filter (taken from mapreduce configuration)
	private int BLOOM_FILTER_SIZE;
	// Writable array for the result of the reducer (i.e. the bloom filter)
	private final BooleanArrayWritable SERIALIZABLE_BLOOM_FILTER = new BooleanArrayWritable();
	// Value to set the corresponding item of the bloom filter in the case of a hit
	private static final boolean HIT_VALUE = true;

	@Override
	public void setup (Context context) {
		// Retrieve configuration
		BLOOM_FILTER_SIZE = context.getConfiguration().getInt("bloom.filter.size", 0);
	}

	@Override
	public void reduce (IntWritable key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException
	{
		// Instantiate the temporary bloom filter, i.e. an array of BooleanWritable
		BooleanWritable[] bloomFilter = new BooleanWritable[BLOOM_FILTER_SIZE];

		// Iterate the intermediate data
		for (IntArrayWritable array : values) {
			// Generate an iterable array
			IntWritable[] intArray = (IntWritable[]) array.toArray();

			// Iterate the list of BF indexes produced by mapper's hash functions
			for (IntWritable i : intArray) {
				int index = i.get();

				// Check if index is a valid number
				if (index >= BLOOM_FILTER_SIZE) {
					System.err.println("[REDUCER]: index " + index +
							" for key " + key + " is not valid");
					continue;
				}

				// Set to true the corresponding item of the array
				if (bloomFilter[index].get() != HIT_VALUE) {
					bloomFilter[index].set(HIT_VALUE);
				}
			}
		}

		// Emit the reducer's results
		SERIALIZABLE_BLOOM_FILTER.set(bloomFilter);
		context.write(key, SERIALIZABLE_BLOOM_FILTER);
	}
}
