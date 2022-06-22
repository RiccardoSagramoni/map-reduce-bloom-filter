package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.builder.BloomFilterUtils;
import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.GenericObject;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ReducerTester
		extends Reducer<ByteWritable, GenericObject, ByteWritable, DoubleWritable>
{

	private static final Logger LOGGER = LogManager.getLogger(ReducerTester.class);

	private final DoubleWritable SERIALIZABLE_FALSE_POSITIVE = new DoubleWritable(0);


	@Override
	public void reduce (ByteWritable key, Iterable<GenericObject> values, Context context)
			throws IOException, InterruptedException {

		LOGGER.info("Reducer key = " + key);

		// Declare the bloomFilter
		BooleanWritable[] bloomFilter = null;

		// Get the bloom filter from the input values
		for (GenericObject object : values) {
			if (object.get() instanceof BooleanArrayWritable) {
				BooleanArrayWritable booleanArrayWritable = (BooleanArrayWritable) object.get();
				bloomFilter = (BooleanWritable[]) booleanArrayWritable.toArray();

				LOGGER.info("bloomFilter = " + Arrays.toString(bloomFilter));
				LOGGER.info("bloomFilter: " + bloomFilter.length);
				break;
			}
		}

		// Check if bloom filter was founded
		if (bloomFilter == null) {
			LOGGER.error("BloomFilter " + key + " doesn't exist");
			return;
		}

		// Restore original state of iterable
		double numberOfTests = 0, numberOfFalsePositives = 0;

		// Get the intermediate results from the mapper
		for (GenericObject object : values) {
			//LOGGER.info("GenericObject for: " + object.get());
			// Skip the bloom filter
			if (object.get() instanceof BooleanArrayWritable) {
				continue;
			}

			// Convert to array of IntWritable (the outputs of the hash functions)
			IntWritable[] intArray = (IntWritable[]) ( (IntArrayWritable)object.get() ).toArray();
			LOGGER.info("intArray: " + Arrays.toString(intArray));

			boolean isFalsePositive = true;

			// Iterate the array of IntWritable in order to check the outputs of the hash functions
			// (i.e. the position to hit in the bloom filter)
			for (IntWritable i : intArray) {
				int index = i.get();
				LOGGER.info("Index=" + index + " key=" + key +
						" BF_size=" + bloomFilter.length);

				if (index < 0 || index >= bloomFilter.length) {
					LOGGER.error("Index " + index + " for key " + key +
							" not valid - out of bound");
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
				LOGGER.info("#falsePositive = " + numberOfFalsePositives);
			}

			LOGGER.info("#tests = " + numberOfTests);
			LOGGER.info("#falsePositive = " + numberOfFalsePositives);
		}

		if (numberOfTests != 0)
			SERIALIZABLE_FALSE_POSITIVE.set(numberOfFalsePositives / numberOfTests);
		context.write(key, SERIALIZABLE_FALSE_POSITIVE);
		LOGGER.info("#tests = " + numberOfTests);
		LOGGER.info("#falsePositive = " + SERIALIZABLE_FALSE_POSITIVE.get());
		LOGGER.info("false positive probability = " + SERIALIZABLE_FALSE_POSITIVE);
	}

}
