package it.unipi.hadoop.line_count;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LineCountMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {

	private final static NullWritable NULL = NullWritable.get();
	private final static IntWritable ONE = new IntWritable(1);

	@Override
	public void map (LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		context.write(NULL, ONE);
	}
}
