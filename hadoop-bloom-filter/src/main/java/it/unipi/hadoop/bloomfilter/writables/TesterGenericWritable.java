package it.unipi.hadoop.bloomfilter.writables;

import org.apache.hadoop.io.GenericWritable;

public class TesterGenericWritable extends GenericWritable {

	private static Class[] CLASSES = {
			BooleanArrayWritable.class, // bloom filter
			IntArrayWritable.class      // hash of the inputs
	};

	protected Class[] getTypes() {
		return CLASSES;
	}
}
