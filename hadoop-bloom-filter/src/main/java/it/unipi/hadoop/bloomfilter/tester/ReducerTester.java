package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.GenericObject;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerTester
		extends Reducer<ByteWritable, GenericObject, ByteWritable, DoubleWritable>
{
	// Size of the bloom filter to be read from the configuration
	private int BLOOM_FILTER_SIZE;
	private final DoubleWritable SERIALIZABLE_FALSE_POSITIVE = new DoubleWritable();

	@Override
	public void setup(Context context){
		BLOOM_FILTER_SIZE = context.getConfiguration().getInt("bloom.filter.size", 0);
	}

	@Override
	public void reduce (ByteWritable key, Iterable<GenericObject> values, Context context)
			throws IOException, InterruptedException{

		// Declare the bloomFilter
		BooleanWritable[] bloomFilter = null;

		// Get the bloom filter from the input values
		for (GenericObject object : values) {
			if (object.get() instanceof BooleanArrayWritable) {
				BooleanArrayWritable booleanArrayWritable = (BooleanArrayWritable) object.get();
				bloomFilter = (BooleanWritable[]) booleanArrayWritable.toArray();
				break;
			}
		}

		// Check if bloom filter was founded
		if (bloomFilter == null) {
			System.err.println("[TEST-REDUCER]: Bloom filter " + key + "doesn't exists");
			return;
		}

		// Restore original state of iterable
		double numberOfTests = 0, numberOfFalsePositives = 0;

		// Get the intermediate results from the mapper
		for (GenericObject object : values) {
			// Skip the bloom filter
			if (object.get() instanceof BooleanWritable) {
				continue;
			}

			// Convert to array of IntWritable (the outputs of the hash functions)
			IntWritable[] intArray = (IntWritable[]) ( (IntArrayWritable)object.get() ).toArray();

			boolean isFalsePositive = true;

			// Iterate the array of IntWritable in order to check the outputs of the hash functions
			// (i.e. the position to hit in the bloom filter)
			for (IntWritable i : intArray) {
				int index = i.get();

				if(index >= BLOOM_FILTER_SIZE){
					System.err.println("[TEST-REDUCER]: Index " + index +
							" out of bound for bloom filter " + key.get());
					return;
				}

				// Check current value
				if (!bloomFilter[index].get()) {
					// If AT LEAST ONE output is NOT set, then the sample is NOT a false positive
					isFalsePositive = false;
					break;
				}

			}

			// Update statistics
			numberOfTests++;
			if (isFalsePositive) {
				numberOfFalsePositives++;
			}

		}

		SERIALIZABLE_FALSE_POSITIVE.set(numberOfFalsePositives / numberOfTests);
		context.write(key, SERIALIZABLE_FALSE_POSITIVE);
	}

}
