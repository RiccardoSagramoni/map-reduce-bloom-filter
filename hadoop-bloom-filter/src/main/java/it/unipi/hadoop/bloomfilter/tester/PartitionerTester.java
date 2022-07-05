package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.tester.writables.IntermediateKeyWritable;
import it.unipi.hadoop.bloomfilter.tester.writables.TesterGenericWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition the output keys of the mapper (IntermediateKeyWritable)
 * by the hash code of their `rating` field<br>
 */
class PartitionerTester extends Partitioner<IntermediateKeyWritable, TesterGenericWritable> {
	@Override
	public int getPartition(IntermediateKeyWritable key,
	                        TesterGenericWritable value,
	                        int numReduceTasks)
	{
		return (key.getRating().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}
