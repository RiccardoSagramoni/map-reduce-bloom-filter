package it.unipi.hadoop.line_count;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LineCountMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

	private final IntWritable RANKING = new IntWritable(0);
	private static final NullWritable NULL = NullWritable.get();

	@Override
	public void map (LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		// TODO Parse input to get ranking @Fabiano
		context.write(RANKING, NULL);
	}
}
