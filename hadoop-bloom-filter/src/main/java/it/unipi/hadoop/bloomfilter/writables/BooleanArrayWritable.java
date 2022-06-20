package it.unipi.hadoop.bloomfilter.writables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;

/**
 * Wrapper for ArrayWritable&lt;BooleanWritable&gt; objects.
 * It's used as output value of the Reducer.
 */
public class BooleanArrayWritable extends ArrayWritable {

	public BooleanArrayWritable() {
		super(BooleanWritable.class);
	}

	@Override
	public String toString() {
		return super.toString();
	}

}
