package it.unipi.hadoop.bloomfilter.writables;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class GenericObject extends GenericWritable {

	private static Class[] CLASSES = {
			BooleanArrayWritable.class, // bloom filter
			IntArrayWritable.class      // hash of the inputs
	};

	public GenericObject() {}

	public GenericObject(Writable instance) {
		set(instance);
	}
	protected Class[] getTypes() {
		return CLASSES;
	}
}
