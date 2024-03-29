package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.util.BloomFilterConfigurationName;
import it.unipi.hadoop.bloomfilter.util.BloomFilterUtils;
import it.unipi.hadoop.bloomfilter.tester.writables.TesterGenericWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import it.unipi.hadoop.bloomfilter.tester.writables.IntermediateKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Mapper of the mapreduce tester application that computes the false positive probability
 * of a bloom filter, given in input.
 * <ul>
 * <li>Input key: id of the line extracted from the input text file (LongWritable)</li>
 * <li>Input value: text of the line (Text)</li>
 * <li>Output key: rating value of the record (ByteWritable)</li>
 * <li>Output value: array with the hash values of the line (TesterGenericWritable)</li>
 * </ul>
 */
class MapperTesterForHashValues
		extends Mapper<LongWritable, Text, IntermediateKeyWritable, TesterGenericWritable>
{
	// Logger
	private static final Logger LOGGER = LogManager.getLogger(MapperTesterForHashValues.class);

	// Instance of the hash function MURMUR_HASH
	private final Hash hash = Hash.getInstance(Hash.MURMUR_HASH);


	// Number of hash functions that must be applied
	private int HASH_FUNCTIONS_NUMBER;
	// Size of each bloom filter
	private Map<Byte, Integer> BLOOM_FILTER_SIZE;


	// Array of IntWritable for the output value of the mapper
	private final IntArrayWritable hashArrayWritable = new IntArrayWritable();

	// Output key (array of indexes to set, FALSE)
	private final IntermediateKeyWritable outputKey = new IntermediateKeyWritable();
	// Generic wrapper for the array of IntWritable hash values
	private final TesterGenericWritable outputValue = new TesterGenericWritable();



	@Override
	public void setup (Context context) {
		// Retrieve configuration
		Configuration configuration = context.getConfiguration();

		// Read size of the bloom filters
		BLOOM_FILTER_SIZE = BloomFilterUtils.readConfigurationBloomFiltersSize(configuration);
		LOGGER.debug("BLOOM_FILTER_SIZE = " + BLOOM_FILTER_SIZE);

		// Read how many hash functions must be implemented
		HASH_FUNCTIONS_NUMBER = context.getConfiguration().getInt(
				BloomFilterConfigurationName.NUMBER_HASH.toString(),
				-1
		);
		LOGGER.debug("Number of hash functions = " + HASH_FUNCTIONS_NUMBER);
	}



	@Override
	public void map (LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		// Create string tokenizer to extract movie id and rating
		StringTokenizer itr = new StringTokenizer(value.toString());

		// Retrieve the tokens obtained
		if (!itr.hasMoreTokens()) {
			LOGGER.error("Input line has not enough tokens: " + value);
			return;
		}
		String movieId = itr.nextToken();

		if (!itr.hasMoreTokens()){
			LOGGER.error("Input line has not enough tokens: " + value);
			return;
		}
		byte rating = (byte) Math.round(Double.parseDouble(itr.nextToken()));



		// Local array of IntWritable to contain the hashes of the movie's id
		IntWritable[] hashes = new IntWritable[HASH_FUNCTIONS_NUMBER];

		// Apply the hash function with different seeds, to obtain HASH_FUNCTIONS_NUMBER values
		for (int i = 0; i < HASH_FUNCTIONS_NUMBER; i++){
			int hashValue = hash.hash(movieId.getBytes(StandardCharsets.UTF_8), i);

			hashes[i] = new IntWritable(
					Math.abs(hashValue % BLOOM_FILTER_SIZE.get(rating))
			);

			LOGGER.debug("Computed hash n." + i + ": " + hashes[i]);
		}

		LOGGER.debug("WRITE (key, value) = ( " + rating + ",  " + Arrays.toString(hashes) + " )");



		// Setting the output values
		outputKey.set(rating, false);
		hashArrayWritable.set(hashes);
		// Wrap the hash values array and send it to the appropriate reducer task
		outputValue.set(hashArrayWritable);

		// Emit the key-value pair
		context.write(outputKey, outputValue);
	}

}
