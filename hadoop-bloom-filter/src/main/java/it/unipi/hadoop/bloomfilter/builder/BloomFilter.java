package it.unipi.hadoop.bloomfilter.builder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

	private static void generateConfiguration (Configuration configuration,
	                                           double falsePositiveProbability,
	                                           Map<Byte, Integer> sizeOfBloomFilters)
	{
		// Set bloomFilter parameters
		configuration.setInt(
				"bloom.filter.number", // how many bloom filter must be created
				sizeOfBloomFilters.size()
		);

		int i = 0;
		for (Map.Entry<Byte, Integer> entry : sizeOfBloomFilters.entrySet()) {
			configuration.setInt(
					"bloom.filter.size.key." + i, // size of k-th bloom filter
					entry.getKey()
			);
			configuration.setInt(
					"bloom.filter.size.value." + i, // size of k-th bloom filter
					entry.getValue()
			);
			i++;
		}

		configuration.setInt(
				"bloom.filter.hash", // how many hash functions for each bloom filter
				computeNumberOfHashFunctions(falsePositiveProbability)
		);

		System.out.println("Number of hash functions: " + computeNumberOfHashFunctions(falsePositiveProbability));
		System.out.println("Rating -> bloom filter size: " + sizeOfBloomFilters);
	}


	/**
	 * TODO
	 * @param configuration
	 * @param falsePositiveProbability
	 * @param inputPath
	 * @param outputPath
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static boolean runBloomFilterBuilder (Configuration configuration,
												  double falsePositiveProbability,
	                                              Path inputPath,
	                                              Path outputPath,
	                                              Map<Byte, Integer> sizeOfBloomFilters)
			throws IOException, ClassNotFoundException, InterruptedException
	{
		// Creation of MapReduce job
		Job job = Job.getInstance(configuration, "BloomFilter");

		generateConfiguration(job.getConfiguration(), falsePositiveProbability, sizeOfBloomFilters);

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
		NLineInputFormat.setInputPaths(job, inputPath);
		job.getConfiguration().setInt(
				"mapreduce.input.lineinputformat.linespermap",
				LINES_PER_MAP
		);

		// Output configuration
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true);
	}


	/**
	 * TODO
	 * @param file
	 * @param falsePositiveProbability
	 * @throws IOException
	 */
	private static Map<Byte, Integer> getBloomFiltersSizeParameters (Configuration conf, Path file,
	                                                                 Double falsePositiveProbability)
			throws IOException
	{
		// Map each rating to the size its corresponding bloom filter
		Map<Byte, Integer> sizeOfBloomFilters = new HashMap<>();

		// Open file in HDFS
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(
						file.getFileSystem(conf).open(file)
				)
			)
		){
			String line;

			// Read each record of the dataset
			while ((line = br.readLine()) != null) {
				// Split the record
				String[] splits = line.split("\\s+");

				// Parse the record and put it in the map
				sizeOfBloomFilters.put(
						Byte.parseByte(splits[0]),
						computeSizeOfBloomFilter(falsePositiveProbability, Integer.parseInt(splits[1]))
				);
			}

		}

		System.out.println("Bloom filter input data = " + sizeOfBloomFilters);
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
		if (otherArgs.length < 4) {
			System.err.println("Usage: BloomFilter <false positive p> <input file> " +
					"<output file> <line count output file>");
			System.exit(2);
		}

		double falsePositiveProbability = Double.parseDouble(otherArgs[0]);
		Path input_file = new Path(otherArgs[1]);
		Path output_file = new Path(otherArgs[2]);
		Path linecount_file = new Path(otherArgs[3]);

		// Read file
		Map<Byte, Integer> sizeOfBloomFilters =
				getBloomFiltersSizeParameters(configuration, linecount_file, falsePositiveProbability);

		boolean succeededJob = runBloomFilterBuilder(
				configuration,
				falsePositiveProbability,
				input_file,
				output_file,
				sizeOfBloomFilters
		);
		if (!succeededJob) {
			System.err.println("BloomFilter failed");
			System.exit(1);
		}
	}
}
