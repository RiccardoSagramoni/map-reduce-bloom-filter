package it.unipi.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class BloomFilterMapper extends Mapper<LongWritable, Text, IntWritable, IntArrayWritable> {

	private int HASH_FUNCTIONS_NUMBER;

	private final IntArrayWritable outputValue = new IntArrayWritable();

	@Override
	public void setup (Mapper.Context context) {
		// Retrieve configuration
		HASH_FUNCTIONS_NUMBER = context.getConfiguration().getInt("bloom.filter.hash", 0);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		IntWritable[] hashes = new IntWritable[HASH_FUNCTIONS_NUMBER];

		StringTokenizer itr = new StringTokenizer(value.toString());
		List<String> list = new ArrayList<>();
		while (itr.hasMoreTokens()) {
			list.add(itr.nextToken());
		}

		String ratingKey = list.get(0);
		String rating = list.get(1);

		Hash hash = Hash.getInstance(Hash.MURMUR_HASH);

		for(int i = 0; i < HASH_FUNCTIONS_NUMBER; i ++){
			int hashValue = hash.hash(ratingKey.getBytes(StandardCharsets.UTF_8),i);
			hashes[i] = new IntWritable(hashValue);
		}

		outputValue.set(hashes);
		IntWritable outputKey = new IntWritable((int) Math.round(Double.parseDouble(rating)));
		context.write(outputKey,outputValue);
	}


}
