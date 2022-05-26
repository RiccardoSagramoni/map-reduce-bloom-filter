package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import it.unipi.hadoop.line_count.LineCountMapper;
import it.unipi.hadoop.line_count.LineCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BloomFilter {

	private static int computeK(double p) {
		int k = (int)(Math.ceil(-(Math.log(p)/(Math.log(2)))));
		return k;
	}

	private static int computeM(double p, int n) {
		int m = (int)(Math.ceil(-(n * Math.log(p)) / (2 * (Math.log(2)))));
		return m;
	}

	private static boolean runBloomFilterBuilder(Configuration conf, double p, int n, Path input_path, Path output_path)
			throws IOException, ClassNotFoundException, InterruptedException {
		// TODO

		Job job = Job.getInstance(conf, "BloomFilter");

		int m = computeM(p, n);
		int k = computeK(p);
		job.getConfiguration().set("bloom.filter.size", String.valueOf(m));	// to set correctly
		job.getConfiguration().set("bloom.filter.hash", String.valueOf(k));

		job.setJarByClass(BloomFilter.class);
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		//iteration.setCombinerClass(BloomFilterCombiner.class);
		job.setReducerClass(BloomFilterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntArrayWritable.class);

		// TODO
		job.setNumReduceTasks(1);		// to set correctly

		// Destination File -> to configure
		// TODO

		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setInputPaths(job, input_path);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 5);		// to set according to the file size

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output_path);

		return job.waitForCompletion(true);
	}

	private static boolean runMapReduceLineCountBuilder(Configuration conf, Path input_path, Path output_path)
			throws IOException, ClassNotFoundException, InterruptedException {

		// Job configuration
		Job job = Job.getInstance(conf, "BloomFilterLineCounter");

		// TODO: set correctly
		job.setJarByClass(BloomFilter.class);
		job.setMapperClass(LineCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(LineCountReducer.class);
		job.setOutputKeyClass(IntWritable.class);				// TODO: VIntWritable or ByteWritable
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setInputPaths(job, input_path);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 5);	// TODO: to set properly

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

		String intermediate_file = otherArgs[3];

		succeded = runMapReduceLineCountBuilder(conf, new Path(otherArgs[1]), new Path(intermediate_file));
		if (!succeded) {
			System.err.println("LineCount failed");
			System.exit(1);
		}

		BufferedReader br = new BufferedReader(new FileReader(intermediate_file));
		String n = br.readLine();
		System.out.println("Firstline is : " + n);
		br.close();

		succeded = runBloomFilterBuilder(conf, Integer.getInteger(otherArgs[0]), Integer.valueOf(n),
				new Path(otherArgs[1]), new Path(otherArgs[2]));
		if (!succeded) {
			System.err.println("BloomFilter failed");
			System.exit(1);
		}

		/*
		// Job configuration
		Job job = Job.getInstance(conf, "BloomFilter");
		double p;	// da terminale
		double n; 	// stimato
		// job.getConfiguration().set("bloom.filter.false", "value p");	// from cmd line
		// job.getConfiguration().set("bloom.filter.keys", "value n");		// to estimate (from line count mapreduce?)
		// compute m and k from expression in the guideline
		job.getConfiguration().set("bloom.filter.size", "value m");	// to set correctly
		job.getConfiguration().set("bloom.filter.hash", "value k");

		job.setJarByClass(BloomFilter.class);
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		//iteration.setCombinerClass(BloomFilterCombiner.class);
		job.setReducerClass(BloomFilterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntArrayWritable.class);

		job.setNumReduceTasks(1);		// to set correctly

		// Destination File -> to configure

		FileInputFormat.addInputPath(job, new Path("INPUT"));
		FileOutputFormat.setOutputPath(job, new Path("OUTPUT"));
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		*/
	}
}
