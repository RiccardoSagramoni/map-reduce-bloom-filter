package it.unipi.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for ArrayWritable&lt;ByteWritable&gt; objects.
 * TODO
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
