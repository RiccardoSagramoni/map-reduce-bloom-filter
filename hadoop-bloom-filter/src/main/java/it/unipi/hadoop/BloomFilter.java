package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import it.unipi.hadoop.line_count.LineCountMapper;
import it.unipi.hadoop.line_count.LineCountReducer;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BloomFilter {

	private static final int NUMBER_OF_REDUCERS = 1;	// TODO set correctly
	private static final int LINES_PER_MAP = 5;			// TODO set according to the file size

	private static int computeK(double p) {
		return (int)(Math.ceil(-(Math.log(p)/(Math.log(2)))));
	}

	private static int computeM(double p, int n) {
		return (int)(Math.ceil(-(n * Math.log(p)) / (2 * (Math.log(2)))));
	}

	private static boolean runBloomFilterBuilder(Configuration conf, double p, int n, Path input_path, Path output_path)
			throws IOException, ClassNotFoundException, InterruptedException {

		// creation of MapReduce job
		Job job = Job.getInstance(conf, "BloomFilter");

		// BloomFilter parameters
		int m = computeM(p, n);
		int k = computeK(p);
		job.getConfiguration().setInt("bloom.filter.size", m);
		job.getConfiguration().setInt("bloom.filter.hash", k);

		job.setJarByClass(BloomFilter.class);

		// mapper configuration
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);		// TODO change with GenericObject ?

		// reducer configuration
		job.setReducerClass(BloomFilterReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);	// TODO change with BooleanArrayWritable
		job.setNumReduceTasks(NUMBER_OF_REDUCERS);

		// input configuration
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setInputPaths(job, input_path);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", LINES_PER_MAP);

		// output configuration
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output_path);

		return job.waitForCompletion(true);
	}

	// TODO: delete this method when implemented another solution
	private static boolean runMapReduceLineCountBuilder(Configuration conf, Path input_path, Path output_path)
			throws IOException, ClassNotFoundException, InterruptedException {

		// Job configuration
		Job job = Job.getInstance(conf, "BloomFilterLineCounter");

		// TODO: set correctly
		job.setJarByClass(BloomFilter.class);
		job.setMapperClass(LineCountMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(LineCountReducer.class);
		job.setOutputKeyClass(IntWritable.class);				// TODO: VIntWritable or ByteWritable
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setInputPaths(job, input_path);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", LINES_PER_MAP);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output_path);

		return job.waitForCompletion(true);
	}

	public static void main (String[] args) throws Exception {
		boolean succeded = true;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Usage: BloomFilter <false positive p> <input file> <output file> <intermediate out file>");
			System.exit(2);
		}

		// output file from lineCount == input file for BloomFilter
		String intermediate_file = otherArgs[3];

		succeded = runMapReduceLineCountBuilder(conf, new Path(otherArgs[1]), new Path(intermediate_file));
		if (!succeded) {
			System.err.println("LineCount failed");
			System.exit(1);
		}

		BufferedReader br = new BufferedReader(new FileReader(intermediate_file));
		String n = br.readLine();
		System.out.println("First line is : " + n);
		br.close();

		succeded = runBloomFilterBuilder(conf, Integer.getInteger(otherArgs[0]), Integer.getInteger(n),
				new Path(otherArgs[1]), new Path(otherArgs[2]));
		if (!succeded) {
			System.err.println("BloomFilter failed");
			System.exit(1);
		}
	}
}
