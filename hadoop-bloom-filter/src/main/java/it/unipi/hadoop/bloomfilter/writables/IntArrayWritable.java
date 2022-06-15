package it.unipi.hadoop.bloomfilter.writables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.DataOutput;
import java.io.IOException;

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
    public IntWritable[] get () {
        return (IntWritable[]) super.get();
    }

    @Override
    public void write (DataOutput dataOutput) throws IOException {
        for(IntWritable data : get()){
            data.write(dataOutput);
        }
    }
}
