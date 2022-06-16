package it.unipi.hadoop.bloomfilter.linecount;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LineCount {

	private static final int NUMBER_OF_REDUCERS = 4;
	private static final int LINES_PER_MAP = 10000;


	private static boolean runLineCounter (Configuration conf, Path input_path, Path output_path)
			throws IOException, ClassNotFoundException, InterruptedException
	{
		// Create MapReduce job
		Job job = Job.getInstance(conf, "LineCounter");

		// Configure JAR
		job.setJarByClass(LineCount.class);

		// Mapper configuration
		job.setMapperClass(LineCountMapper.class);
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		// Reducer configuration
		job.setReducerClass(LineCountReducer.class);
		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(NUMBER_OF_REDUCERS);

		// Input configuration
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setInputPaths(job, input_path);
		job.getConfiguration().setInt(
				"mapreduce.input.lineinputformat.linespermap",
				LINES_PER_MAP
		);

		// Output configuration
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output_path);

		return job.waitForCompletion(true);
	}



	public static void main (String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: BloomFilter <false positive p> <input file> <output file>");
			System.exit(2);
		}

		boolean succeeded = runLineCounter(
				conf,
				new Path(otherArgs[1]),
				new Path(otherArgs[2])
		);
		if (!succeeded) {
			System.err.println("LineCount failed");
			System.exit(1);
		}

	}
}
