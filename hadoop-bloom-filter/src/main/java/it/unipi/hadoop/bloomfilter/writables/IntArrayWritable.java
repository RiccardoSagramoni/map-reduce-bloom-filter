package it.unipi.hadoop.bloomfilter.writables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Wrapper for ArrayWritable&lt;IntWritable&gt; objects.
 * It's used as output value of the Mapper, so that the Reducer can
 * correctly retrieve the value.
 */
public class IntArrayWritable extends ArrayWritable {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	@Override
	public String toString() {
		return super.toString();
	}

}
