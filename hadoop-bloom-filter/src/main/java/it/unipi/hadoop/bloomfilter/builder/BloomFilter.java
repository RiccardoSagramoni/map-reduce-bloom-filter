package it.unipi.hadoop.bloomfilter.builder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BloomFilter {

	private static final int NUMBER_OF_REDUCERS = 4;
	private static final int LINES_PER_MAP = 10000;

	/**
	 * TODO
	 * @param falsePositiveProbability
	 * @return
	 */
	private static int computeNumberOfHashFunctions (double falsePositiveProbability) {
		return (int)(Math.ceil(-(Math.log(falsePositiveProbability)/(Math.log(2)))));
	}


	/**
	 * TODO
	 * @param falsePositiveProbability
	 * @param numberOfInputs
	 * @return
	 */
	private static int computeSizeOfBloomFilter (double falsePositiveProbability, int numberOfInputs) {
		return (int)(Math.ceil(-(numberOfInputs * Math.log(falsePositiveProbability)) / (2 * (Math.log(2)))));
	}


	/**
	 * TODO
	 * @param conf
	 * @param falsePositiveProbability
	 * @param input_path
	 * @param output_path
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static boolean runBloomFilterBuilder (Configuration conf,
												  double falsePositiveProbability,
	                                              Path input_path,
	                                              Path output_path,
	                                              Map<Byte, Integer> sizeOfBloomFilters)
			throws IOException, ClassNotFoundException, InterruptedException
	{
		// Creation of MapReduce job
		Job job = Job.getInstance(conf, "BloomFilter");

		// Set bloomFilter parameters
		job.getConfiguration().setInt(
				"bloom.filter.number", // how many bloom filter must be created
				sizeOfBloomFilters.size()
		);
		sizeOfBloomFilters.forEach( (k, v) ->
				job.getConfiguration().setInt(
						"bloom.filter.size." + k, // size of k-th bloom filter
						v
				)
		);
		job.getConfiguration().setInt(
				"bloom.filter.hash", // how many hash functions for each bloom filter
				computeNumberOfHashFunctions(falsePositiveProbability)
		);

		// Configure JAR
		job.setJarByClass(BloomFilter.class);

		// Mapper configuration
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);

		// Reducer configuration
		job.setReducerClass(BloomFilterReducer.class);
		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(BooleanArrayWritable.class);
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


	/**
	 * TODO
	 * @param file
	 * @param falsePositiveProbability
	 * @throws IOException
	 */
	private static Map<Byte, Integer> getBloomFiltersSizeParameters (Path file,
	                                                                 Double falsePositiveProbability)
			throws IOException
	{
		Map<Byte, Integer> sizeOfBloomFilters = new HashMap<>();
		ByteWritable key = new ByteWritable();
		IntWritable value = new IntWritable();

		try (SequenceFile.Reader reader =
				    new SequenceFile.Reader(
							new Configuration(),
						    SequenceFile.Reader.file(file)
				    )
		){
			while (reader.next(key, value)) {
				sizeOfBloomFilters.put(
						key.get(),
						computeSizeOfBloomFilter(falsePositiveProbability, value.get())
				);
			}
		}

		return sizeOfBloomFilters;
	}


	/**
	 * TODO
	 * @param args
	 * @throws Exception
	 */
	public static void main (String[] args) throws Exception {

		// Create configuration
		Configuration configuration = new Configuration();

		// Parse application arguments
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: BloomFilter <false positive p> <input file> " +
					"<output file>");
			System.exit(2);
		}

		double falsePositiveProbability = Double.parseDouble(otherArgs[0]);
		Path input_file = new Path(otherArgs[1]);
		Path output_file = new Path(otherArgs[2]);

		// Read file
		Map<Byte, Integer> sizeOfBloomFilters =
				getBloomFiltersSizeParameters(input_file, falsePositiveProbability);

		boolean succeded = runBloomFilterBuilder(
				configuration,
				falsePositiveProbability,
				input_file,
				output_file,
				sizeOfBloomFilters
		);
		if (!succeded) {
			System.err.println("BloomFilter failed");
			System.exit(1);
		}
	}
}
