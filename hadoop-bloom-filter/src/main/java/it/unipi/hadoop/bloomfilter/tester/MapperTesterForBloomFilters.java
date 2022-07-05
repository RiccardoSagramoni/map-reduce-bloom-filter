package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.tester.writables.IntermediateKeyWritable;
import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.tester.writables.TesterGenericWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Second mapper of the mapreduce tester application that
 * reads the output of the builder (i.e. the bloom filter)
 * and maps it according to the key (i.e. the rating value).<br>
 * It simply distributes the bloom filters to the reducer tasks, so that
 * they can be tested.
 * <ul>
 * <li>Input key: rating value of the record (ByteWritable)</li>
 * <li>Input value: bloom filter structure (BooleanArrayWritable)</li>
 * <li>Output key: rating value of the record (ByteWritable)</li>
 * <li>Output value: bloom filter wrapped in a different structure (TesterGenericWritable)</li>
 * </ul>
 */
class MapperTesterForBloomFilters
		extends Mapper<ByteWritable, BooleanArrayWritable, IntermediateKeyWritable, TesterGenericWritable>
{
	// Output key (extracted bloom filter, TRUE)
	private final IntermediateKeyWritable outputKey = new IntermediateKeyWritable();
	// Generic wrapper for the bloom filter
	private final TesterGenericWritable outputValue = new TesterGenericWritable();

	@Override
	public void map (ByteWritable key, BooleanArrayWritable value, Context context)
			throws IOException, InterruptedException
	{
		// Wrap the key and the bloom filter and send it to the appropriate reducer task
		outputKey.set(key.get(), true);
		outputValue.set(value);
		context.write(outputKey, this.outputValue);
	}

}
