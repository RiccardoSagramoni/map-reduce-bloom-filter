package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BloomFilter {
	public static void main (String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2)	// ??
			System.err.println("Usage: BloomFilter [input] [output]"); System.exit(2);

		// Job configuration
		Job job = Job.getInstance(conf, "BloomFilter");
		job.getConfiguration().set("bloom.filter.false", "value p");	// from cmd line
		job.getConfiguration().set("bloom.filter.keys", "value n");		// to estimate
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
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
