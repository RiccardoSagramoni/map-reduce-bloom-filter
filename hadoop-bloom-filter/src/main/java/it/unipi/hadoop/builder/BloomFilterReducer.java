package it.unipi.hadoop.builder;

import it.unipi.hadoop.writables.BooleanArrayWritable;
import it.unipi.hadoop.writables.IntArrayWritable;
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
	private int BLOOM_FILTER_SIZE;
	// Writable array for the result of the reducer (i.e. the bloom filter)
	private final BooleanArrayWritable SERIALIZABLE_BLOOM_FILTER = new BooleanArrayWritable();

	@Override
	public void setup (Context context) {
		// Retrieve configuration
		BLOOM_FILTER_SIZE = context.getConfiguration().getInt("bloom.filter.size", 0);
	}

	@Override
	public void reduce (ByteWritable key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException
	{


		// Instantiate the temporary bloom filter, i.e. an array of BooleanWritable
		BooleanWritable[] bloomFilter = new BooleanWritable[BLOOM_FILTER_SIZE];

		// Iterate the intermediate data
		for (IntArrayWritable array : values) {
			// Generate an iterable array
			IntWritable[] arrayWithHashedIndexes = (IntWritable[]) array.toArray();

			// Iterate the list of BF indexes produced by mapper's hash functions
			for (IntWritable i : arrayWithHashedIndexes) {
				int indexToSet = i.get();

				// Check if index is a valid number
				if (indexToSet >= BLOOM_FILTER_SIZE) {
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
