package it.unipi.hadoop.bloomfilter.builder;

import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

public class BloomFilterMapper extends Mapper<LongWritable, Text, ByteWritable, IntArrayWritable> {

	// Number of hash functions that must be applied for each rating
	private int[] HASH_FUNCTIONS_NUMBER;

	//Array of IntWritable for the output value of the mapper
	private final IntArrayWritable outputValue = new IntArrayWritable();

	//ByteWritable for the output key
	private final ByteWritable outputKey = new ByteWritable();

	//Instance of the hash function MURMUR_HASH
	private final Hash hash = Hash.getInstance(Hash.MURMUR_HASH);

	//Array that must contain the movie's id and the rounded rating of it
	private final String[] tokens  = new String[2];


	@Override
	public void setup (Mapper.Context context) {
		// Retrieve configuration
		Configuration configuration = context.getConfiguration();

		// Read how many bloom filters will be created and allocate the array for the hash functions
		int howManyBloomFilters = configuration.getInt("bloom.filter.number", -1);
		if (howManyBloomFilters <= 0) {
			System.err.println("[MAPPER]: bloom.filter.number parameter not set");
			return;
		}
		HASH_FUNCTIONS_NUMBER = new int[howManyBloomFilters];

		// Read how many hash functions are required for each bloom filter
		// and build the array HASH_FUNCTIONS_NUMBER
		for (int i = 0; i < howManyBloomFilters; i++) {
			int howManyHashFunctions = configuration.getInt(
					"bloom.filter.hash." + i,
					-1
			);
			if (howManyHashFunctions <= 0) {
				System.err.println("[MAPPER]: bloom.filter.hash." + i + " parameter not set");
				return;
			}
			HASH_FUNCTIONS_NUMBER[i] = howManyHashFunctions;
		}

	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());
		//Retrieve the tokens obtained
		if (itr.hasMoreTokens()) {
			tokens[0] = itr.nextToken();
		}else{
			return;
		}
		if(itr.hasMoreTokens()){
			tokens[1] = itr.nextToken();
		}else{
			return;
		}

		String movieId = tokens[0];
		byte rating = (byte) Math.round(Double.parseDouble(tokens[1]));

		//Local array of IntWritable to contain the hashes of the movie's id
		int hashFunctionsNumber = HASH_FUNCTIONS_NUMBER[rating];
		IntWritable[] hashes = new IntWritable[hashFunctionsNumber];

		//Apply the hash function with different seeds, to obtain HASH_FUNCTIONS_NUMBER values
		for(int i = 0; i < hashFunctionsNumber; i ++){
			int hashValue = hash.hash(movieId.getBytes(StandardCharsets.UTF_8), i);
			hashes[i] = new IntWritable(hashValue);
		}

		//Setting the output values
		outputKey.set(rating);
		outputValue.set(hashes);

		//Emit the k-v pair
		context.write(outputKey,outputValue);
	}
}
