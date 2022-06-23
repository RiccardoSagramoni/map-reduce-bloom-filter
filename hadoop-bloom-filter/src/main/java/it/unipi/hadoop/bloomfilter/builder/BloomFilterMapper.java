package it.unipi.hadoop.bloomfilter.builder;

import it.unipi.hadoop.bloomfilter.util.BloomFilterUtils;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;

public class BloomFilterMapper extends Mapper<LongWritable, Text, ByteWritable, IntArrayWritable> {
	// Logger
	private static final Logger LOGGER = LogManager.getLogger(BloomFilterMapper.class);

	// Number of hash functions that must be applied
	private int HASH_FUNCTIONS_NUMBER;
	// Size of each bloom filter
	private Map<Byte, Integer> BLOOM_FILTER_SIZE;

	// Array of IntWritable for the output value of the mapper
	private final IntArrayWritable outputValue = new IntArrayWritable();
	// ByteWritable for the output key
	private final ByteWritable outputKey = new ByteWritable();

	// Instance of the hash function MURMUR_HASH
	private final Hash hash = Hash.getInstance(Hash.MURMUR_HASH);



	@Override
	public void setup (Context context) {
		// Retrieve configuration
		Configuration configuration = context.getConfiguration();

		// Read size of the bloom filters
		BLOOM_FILTER_SIZE = BloomFilterUtils.readConfigurationBloomFiltersSize(configuration);

		// Read how many hash functions must be implemented
		HASH_FUNCTIONS_NUMBER = context.getConfiguration().getInt(
				"bloom.filter.hash",
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
		LOGGER.debug("movieId = " + movieId);

		if (!itr.hasMoreTokens()){
			LOGGER.error("Input line has not enough tokens: " + value);
			return;
		}
		byte rating = (byte) Math.round(Double.parseDouble(itr.nextToken()));
		LOGGER.debug("Rating = " + rating);




		Integer bloomFilterSize = BLOOM_FILTER_SIZE.get(rating);
		if (bloomFilterSize == null) {
			LOGGER.error("Rating key " + rating + " doesn't exist in linecount");
			return;
		}

		// Local array of IntWritable to contain the hashes of the movie's id
		IntWritable[] hashes = new IntWritable[HASH_FUNCTIONS_NUMBER];

		// Apply the hash function with different seeds, to obtain HASH_FUNCTIONS_NUMBER values
		for (int i = 0; i < HASH_FUNCTIONS_NUMBER; i++){
			int hashValue = hash.hash(movieId.getBytes(StandardCharsets.UTF_8), i);

			hashes[i] = new IntWritable(
					Math.abs(hashValue % bloomFilterSize)
			);

			LOGGER.debug("Computed hash n." + i + ": " + hashes[i]);
		}

		LOGGER.debug("WRITE (key, value) = ( " + rating + ",  " + Arrays.toString(hashes) + " )");



		// Setting the output values
		outputKey.set(rating);
		outputValue.set(hashes);

		// Emit the key-value pair
		context.write(outputKey, outputValue);
	}

}
