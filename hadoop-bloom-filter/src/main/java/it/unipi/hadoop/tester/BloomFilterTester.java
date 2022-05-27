package it.unipi.hadoop.tester;

import it.unipi.hadoop.writables.GenericObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

// TODO change IntWritable for output mapper to ByteWritable or VIntWritable

public class BloomFilterTester {

	private static final int NUMBER_OF_REDUCERS = 1; // TODO set correctly
	private static final int LINES_PER_MAP = 5; // TODO set correctly


	public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) { // TODO
			System.err.println("Usage: BloomFilter <false positive p> <input file> <output file> <intermediate out file>");
			System.exit(2);
		}

		// Create MapReduce job
		Job job = Job.getInstance(conf, "BloomFilter Tester");
		job.setJarByClass(ReducerTester.class);

		// todo input format set

		// Configure mapper which distributes the dataset
		MultipleInputs.addInputPath(
				job,
				new Path(args[0]),
				NLineInputFormat.class,
				MapperTester1.class
		);
		job.getConfiguration().setInt(
				"mapreduce.input.lineinputformat.linespermap",
				LINES_PER_MAP);

		// Configure mapper which distributes the bloom filters
		MultipleInputs.addInputPath(
				job,
				new Path(args[1]),
				// TODO multiple files??? (probably not: mapreduce automatically handles the partition
				SequenceFileInputFormat.class,
				MapperTester2.class
		);

		// Configure output key/value for mappers
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(GenericObject.class);


		// Configure reducer
		job.setReducerClass(ReducerTester.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(NUMBER_OF_REDUCERS);


		// Configure output path
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
