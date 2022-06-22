package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.writables.GenericObject;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class BloomFilterTester {

	private static final int NUMBER_OF_REDUCERS = 4;
	private static final int LINES_PER_MAP = 10000;

	/**
	 * TODO
	 * @param falsePositiveProbability
	 * @return
	 */
	private static int computeNumberOfHashFunctions (double falsePositiveProbability) {
		return (int)(Math.ceil(
				-(Math.log(falsePositiveProbability) / (Math.log(2)))
		));
	}


	/**
	 * TODO
	 * @param falsePositiveProbability
	 * @param numberOfInputs
	 * @return
	 */
	private static int computeSizeOfBloomFilter (double falsePositiveProbability, int numberOfInputs) {
		return (int)(Math.ceil(
				-(numberOfInputs * Math.log(falsePositiveProbability)) / Math.pow(Math.log(2), 2)
		));
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
					"bloom.filter.size.key." + i, // rating key of the k-th bloom filter
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
	 * @param input_dataset
	 * @param output_bloom_filter
	 * @param output_tester
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static boolean runBloomFilterTester (Configuration configuration,
												  double falsePositiveProbability,
												  Path input_dataset,
												  Path output_bloom_filter,
												  Path output_tester,
												  Map<Byte, Integer> sizeOfBloomFilters)
			throws IOException, ClassNotFoundException, InterruptedException
	{

		// Create MapReduce job
		Job job = Job.getInstance(configuration, "BloomFilter Tester");
		generateConfiguration(job.getConfiguration(), falsePositiveProbability, sizeOfBloomFilters);

		job.setJarByClass(BloomFilterTester.class);


		// Configure mapper which distributes the dataset
		MultipleInputs.addInputPath(
				job,
				input_dataset,
				NLineInputFormat.class,
				MapperTester.class // Reuse mapper for building bloom filter
		);
		job.getConfiguration().setInt(
				"mapreduce.input.lineinputformat.linespermap",
				LINES_PER_MAP);

		// Configure mapper which distributes the bloom filters
		MultipleInputs.addInputPath(
				job,
				output_bloom_filter,
				// TODO multiple files??? (probably not: mapreduce automatically handles the partition)
				SequenceFileInputFormat.class,
				MapperTesterForBloomFilters.class
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
		TextOutputFormat.setOutputPath(job, output_tester);

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

	public static void main (String[] args) 
				throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length < 5) {
			System.err.println("Usage: BloomFilterTester <false positive p> <dataset file for testing> " +
					"<file with the bloom filters> <output file> <linecount file>");
			System.exit(1);
		}

		double falsePositiveProbability = Double.parseDouble(otherArgs[0]);
		Path input_dataset = new Path(otherArgs[1]);
		Path output_bloom_filter = new Path(otherArgs[2]);
		Path output_tester = new Path(otherArgs[3]);
		Path linecount_output = new Path(otherArgs[4]);

		// Read file
		Map<Byte, Integer> sizeOfBloomFilters =
				getBloomFiltersSizeParameters(configuration, linecount_output, falsePositiveProbability);

		boolean succeeded = runBloomFilterTester(
				configuration,
				falsePositiveProbability,
				input_dataset, output_bloom_filter, output_tester,
				sizeOfBloomFilters);

		if (!succeeded) {
			System.err.println("BloomFilter Tester failed");
			System.exit(1);
		}
	}

}
