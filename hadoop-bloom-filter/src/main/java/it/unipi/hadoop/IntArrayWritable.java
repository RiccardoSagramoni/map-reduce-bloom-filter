package it.unipi.hadoop;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.DataOutput;
import java.io.IOException;

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
