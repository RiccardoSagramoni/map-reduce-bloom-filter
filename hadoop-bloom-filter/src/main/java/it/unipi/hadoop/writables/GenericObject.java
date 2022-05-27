package it.unipi.hadoop.writables;

import it.unipi.hadoop.BooleanArrayWritable;
import it.unipi.hadoop.IntArrayWritable;
import org.apache.hadoop.io.GenericWritable;

public class GenericObject extends GenericWritable {

	private static Class[] CLASSES = {
			BooleanArrayWritable.class, // bloom filter
			IntArrayWritable.class      // hash of the inputs
	};

	protected Class[] getTypes() {
		return CLASSES;
	}
}
