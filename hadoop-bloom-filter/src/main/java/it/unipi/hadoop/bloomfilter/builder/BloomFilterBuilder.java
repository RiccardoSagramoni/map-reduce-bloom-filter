package it.unipi.hadoop.bloomfilter.builder;

import java.io.IOException;
import java.util.Map;

import it.unipi.hadoop.bloomfilter.util.BloomFilterUtils;
import it.unipi.hadoop.bloomfilter.util.MapReduceParameters;
import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Launch the MapReduce job for generating the bloom filters
 * from a given dataset.<br><br>
 * Use the following command to launch the application: <br>
 * <code>
 *     hadoop jar <i>application.jar</i> it.unipi.hadoop.bloomfilter.builder.BloomFilterBuilder
 *     	<i>&#60;false_positive_probability&#62;</i> <i>training_dataset_path</i>
 *     	<i>output_path</i> <i>linecount_output_path</i>
 * </code>
 */
public class BloomFilterBuilder {

	/**
	 * Run the MapReduce job for building the bloom filters
	 * @param configuration job configuration
	 * @param falsePositiveProbability desired false positive probability
	 * @param inputPath path to the input dataset
	 * @param outputPath path to the output location
	 * @param sizeOfBloomFilters map with the size of the corresponding bloom filter for each rating value
	 * @return true on success, false on failure
	 */
	private static boolean runBloomFilterBuilder (Configuration configuration,
												  double falsePositiveProbability,
	                                              Path inputPath,
	                                              Path outputPath,
	                                              Map<Byte, Integer> sizeOfBloomFilters)
			throws IOException, ClassNotFoundException, InterruptedException
	{
		// Creation of MapReduce job
		Job job = Job.getInstance(configuration, "HADOOP_BLOOM_FILTERS_BUILDER");

		BloomFilterUtils.generateConfiguration(job.getConfiguration(), falsePositiveProbability, sizeOfBloomFilters);

		// Configure JAR
		job.setJarByClass(BloomFilterBuilder.class);

		// Mapper configuration
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);

		// Reducer configuration
		job.setReducerClass(BloomFilterReducer.class);
		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(BooleanArrayWritable.class);
		job.setNumReduceTasks(MapReduceParameters.getInstance().getNumberOfReducersBuilder());

		// Input configuration
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setInputPaths(job, inputPath);
		NLineInputFormat.setNumLinesPerSplit(
				job,
				MapReduceParameters.getInstance().getLinesPerMapBuilder()
		);

		// Output configuration
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true);
	}


	public static void main (String[] args) throws Exception {

		// Create configuration
		Configuration configuration = new Configuration();

		// Parse application arguments
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Required arguments: <false positive p> <input file> " +
					"<line count output file> <output file>");
			System.exit(2);
		}

		double falsePositiveProbability = Double.parseDouble(otherArgs[0]);
		Path input_file = new Path(otherArgs[1]);
		Path linecount_file = new Path(otherArgs[2]);
		Path output_file = new Path(otherArgs[3]);

		// Compute the size of bloom filters
		Map<Byte, Integer> sizeOfBloomFilters =
				BloomFilterUtils.getBloomFiltersSizeParameters(
						configuration,
						linecount_file,
						falsePositiveProbability
				);

		// Launch the MapReduce job
		boolean succeededJob = runBloomFilterBuilder(
				configuration,
				falsePositiveProbability,
				input_file,
				output_file,
				sizeOfBloomFilters
		);
		if (!succeededJob) {
			System.err.println("Bloom filters builder failed");
			System.exit(1);
		}
	}
}
