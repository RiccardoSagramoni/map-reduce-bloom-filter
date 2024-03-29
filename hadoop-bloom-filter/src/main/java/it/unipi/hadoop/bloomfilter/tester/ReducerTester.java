package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.tester.writables.IntermediateKeyWritable;
import it.unipi.hadoop.bloomfilter.tester.writables.TesterGenericWritable;
import it.unipi.hadoop.bloomfilter.tester.writables.TesterResultsWritable;
import it.unipi.hadoop.bloomfilter.writables.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * Reducer of the mapreduce tester application that computes the false probability
 * of a bloom filter, given in input.
 * <ul>
 * <li>Input key: average rating (ByteWritable)</li>
 * <li>Input value: output of two mappers (TesterGenericWritable):
 * <ul>
 *     <li>Bloom filter structure, from the builder (BooleanArrayWritable)</li>
 *     <li>Array of position to check if set to 1 in the given bloom filter (IntArrayWritable)</li>
 * </ul></li>
 * <li>Output key: average rating (ByteWritable)</li>
 * <li>Output value: result of the tester (TesterResultsWritable)
 *      <ul>
 *          <li>Number of false positives (int)</li>
 *          <li>Total number of test samples (int)</li>
 *          <li>False positive probability</li>
 *      </ul>
 * </li>
 * </ul>
 */
class ReducerTester
		extends Reducer<IntermediateKeyWritable, TesterGenericWritable, ByteWritable, TesterResultsWritable>
{
	// Logger
	private static final Logger LOGGER = LogManager.getLogger(ReducerTester.class);

	private final ByteWritable outputKey = new ByteWritable();

	// Result of tester execution
	// (number of false positives, total number of test samples, false positive probability)
	private final TesterResultsWritable testResults = new TesterResultsWritable();


	@Override
	public void reduce (IntermediateKeyWritable key, Iterable<TesterGenericWritable> values, Context context)
			throws IOException, InterruptedException
	{
		LOGGER.debug("Reducer key = " + key);

		// Get the bloom filter from the input values
		TesterGenericWritable wrappedBloomFilter = values.iterator().next();
		if (!(wrappedBloomFilter.get() instanceof BooleanArrayWritable)) {
			LOGGER.error("BloomFilter " + key + " doesn't exist");
			return;
		}

		BooleanWritable[] bloomFilter = (BooleanWritable[])
						( (BooleanArrayWritable) wrappedBloomFilter.get() )
						.toArray();

		LOGGER.debug("bloomFilter = " + Arrays.toString(bloomFilter));
		LOGGER.debug("bloomFilter length = " + bloomFilter.length);


		int numberOfTests = 0, numberOfFalsePositives = 0;

		// Get the intermediate results from the mapper
		for (TesterGenericWritable object : values) {

			// Convert to array of IntWritable (the outputs of the hash functions)
			IntWritable[] intArray = (IntWritable[]) ( (IntArrayWritable)object.get() ).toArray();
			LOGGER.debug("intArray = " + Arrays.toString(intArray));

			boolean isFalsePositive = true;

			// Iterate the array of IntWritable in order to check the outputs of the hash functions
			// (i.e. the position to hit in the bloom filter)
			for (IntWritable i : intArray) {
				int index = i.get();
				LOGGER.debug("Index = " + index + " key = " + key +
						" BF_size = " + bloomFilter.length);

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
			}

		}

		// Set the results of the reducer
		outputKey.set(key.getRating());
		testResults.set(numberOfFalsePositives, numberOfTests);
		context.write(outputKey, testResults);

		LOGGER.debug("#tests = " + numberOfTests);
		LOGGER.debug("#falsePositive = " + numberOfFalsePositives);
		LOGGER.debug("key = " + key + ", false positive probability = " + testResults);
	}

}
