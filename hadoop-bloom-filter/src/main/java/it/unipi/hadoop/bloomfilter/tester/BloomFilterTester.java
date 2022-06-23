package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.util.BloomFilterUtils;
import it.unipi.hadoop.bloomfilter.writables.TesterGenericWritable;
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
import java.util.Map;

/**
 * Launch the MapReduce job for generating the bloom filters
 * from a given dataset.<br><br>
 * Use the following command to launch the application: <br>
 * <code>
 *     hadoop jar <i>application.jar</i> it.unipi.hadoop.bloomfilter.tester.BloomFilterTester
 *     	<i>&#60;false_positive_probability&#62;</i> <i>test_dataset_path</i>
 *     	<i>bloom_filters_path</i>
 *     	<i>output_path</i>
 *     	<i>linecount_output_path</i>
 * </code>
 * <br><br>
 * The <i>bloom_filters_path</i> is the output of the builder job.<br>
 * The <i>linecount_output_path</i> is the output of the MapReduce job which counts the
 * number of keys for each bloom filter for the <b>TRAINING</b> partition of the dataset.
 */
public class BloomFilterTester {

	private static final int NUMBER_OF_REDUCERS = 4;
	private static final int LINES_PER_MAP = 10000;

	/**
	 * Run the MapReduce job for testing the bloom filters
	 * @param configuration job configuration
	 * @param falsePositiveProbability desired false positive probability
	 * @param inputDatasetPath path to the input dataset
	 * @param bloomFiltersPath path to the file with the bloom filter (i.e. the output of the builder)
	 * @param outputTesterPath path to the output location
	 * @param sizeOfBloomFilters map with the size of the corresponding bloom filter for each rating value
	 * @return true on success, false on failure
	 */
	private static boolean runBloomFilterTester (Configuration configuration,
	                                             double falsePositiveProbability,
	                                             Path inputDatasetPath,
												 Path bloomFiltersPath,
												 Path outputTesterPath,
												 Map<Byte, Integer> sizeOfBloomFilters)
			throws IOException, ClassNotFoundException, InterruptedException
	{

		// Create MapReduce job
		Job job = Job.getInstance(configuration, "BloomFilter Tester");
		BloomFilterUtils.generateConfiguration(
				job.getConfiguration(),
				falsePositiveProbability,
				sizeOfBloomFilters
		);

		// Set JAR
		job.setJarByClass(BloomFilterTester.class);


		// Configure mapper which distributes the dataset
		MultipleInputs.addInputPath(
				job,
				inputDatasetPath,
				NLineInputFormat.class,
				MapperTesterForHashValues.class // Reuse mapper for building bloom filter
		);
		job.getConfiguration().setInt(
				"mapreduce.input.lineinputformat.linespermap",
				LINES_PER_MAP);

		// Configure mapper which distributes the bloom filters
		MultipleInputs.addInputPath(
				job,
				bloomFiltersPath,
				SequenceFileInputFormat.class,
				MapperTesterForBloomFilters.class
		);

		// Configure output key/value for mappers
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(TesterGenericWritable.class);


		// Configure reducer
		job.setReducerClass(ReducerTester.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(NUMBER_OF_REDUCERS);


		// Configure output path
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputTesterPath);

		return job.waitForCompletion(true);
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

		// Compute the size of bloom filters
		Map<Byte, Integer> sizeOfBloomFilters =
				BloomFilterUtils.getBloomFiltersSizeParameters(
						configuration,
						linecount_output,
						falsePositiveProbability
				);

		// Launch the MapReduce job
		boolean succeeded = runBloomFilterTester(
				configuration,
				falsePositiveProbability,
				input_dataset, output_bloom_filter, output_tester,
				sizeOfBloomFilters
		);
		if (!succeeded) {
			System.err.println("BloomFilter Tester failed");
			System.exit(1);
		}
	}

}
