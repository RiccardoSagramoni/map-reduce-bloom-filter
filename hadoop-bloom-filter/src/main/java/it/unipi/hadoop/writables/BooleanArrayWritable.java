package it.unipi.hadoop.writables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for ArrayWritable&lt;BooleanWritable&gt; objects.
 * It's used as output value of the Reducer.
 */
public class BooleanArrayWritable extends ArrayWritable {

	public BooleanArrayWritable() {
		super(BooleanWritable.class);
	}

	@Override
	public BooleanWritable[] get () {
		return (BooleanWritable[]) super.get();
	}
	
	@Override
	public void write (DataOutput dataOutput) throws IOException {
		for(BooleanWritable data : get()){
			data.write(dataOutput);
		}
	}
}
