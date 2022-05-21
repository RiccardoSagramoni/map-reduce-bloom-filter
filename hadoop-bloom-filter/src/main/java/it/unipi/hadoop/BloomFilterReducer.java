package it.unipi.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BloomFilterReducer extends Reducer<Text, Text, NullWritable, Text>  {
	@Override
	public void reduce (Text key, Iterable<Text> values, Context context) {

	}
}
