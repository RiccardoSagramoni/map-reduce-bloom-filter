package it.unipi.hadoop.line_count;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LineCountReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

	private final IntWritable result = new IntWritable();

	@Override
	public void reduce (IntWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException
	{
		// Count the input  (i.e. the lines in the file)
		int sum = 0;
		for (NullWritable v : values) {
			sum++;
		}

		// Write the result
		result.set(sum);
		context.write(key, result);
	}
}
