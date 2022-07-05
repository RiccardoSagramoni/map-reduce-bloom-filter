package it.unipi.hadoop.bloomfilter.tester.writables;

import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.IntArrayWritable;
import org.apache.hadoop.io.GenericWritable;

/**
 * Generic wrapper for the output of the mappers.
 * <br>
 * In this way, we can distribute the bloom filters with the arrays of indexes, in an efficient way
 * (i.e. through the `shuffle & sort` operation)
 */
public class TesterGenericWritable extends GenericWritable {

	private static Class[] CLASSES = {
			BooleanArrayWritable.class, // bloom filter
			IntArrayWritable.class      // hash of the inputs (indexes to set)
	};

	protected Class[] getTypes() {
		return CLASSES;
	}
}
