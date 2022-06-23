package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.TesterGenericWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperTesterForBloomFilters
		extends Mapper<ByteWritable, BooleanArrayWritable, ByteWritable, TesterGenericWritable>
{
	private final TesterGenericWritable object = new TesterGenericWritable();

	@Override
	public void map (ByteWritable key, BooleanArrayWritable value, Context context)
			throws IOException, InterruptedException
	{
		object.set(value);
		context.write(key, object);
	}

}
