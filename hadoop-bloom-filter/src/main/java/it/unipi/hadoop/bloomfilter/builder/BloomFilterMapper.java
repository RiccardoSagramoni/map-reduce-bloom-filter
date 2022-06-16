package it.unipi.hadoop.bloomfilter.builder;

import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

public class BloomFilterMapper extends Mapper<LongWritable, Text, ByteWritable, IntArrayWritable> {

	//Number of hash functions that must be applied
	private int HASH_FUNCTIONS_NUMBER;

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
		HASH_FUNCTIONS_NUMBER = context.getConfiguration().getInt("bloom.filter.hash", 0);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//Local array of IntWritable to contain the hashes of the movie's id
		IntWritable[] hashes = new IntWritable[HASH_FUNCTIONS_NUMBER];


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

		String ratingKey = tokens[0];
		String rating = tokens[1];

		//Apply the hash function with different seeds, to obtain HASH_FUNCTIONS_NUMBER values
		for(int i = 0; i < HASH_FUNCTIONS_NUMBER; i ++){
			int hashValue = hash.hash(ratingKey.getBytes(StandardCharsets.UTF_8),i);
			hashes[i] = new IntWritable(hashValue);
		}

		//Setting the output values
		outputValue.set(hashes);
		outputKey.set((byte) Math.round(Double.parseDouble(rating)));

		//Emit the k-v pair
		context.write(outputKey,outputValue);
	}
}
