package it.unipi.hadoop.bloomfilter.linecount;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class LineCountMapper extends Mapper<LongWritable, Text, ByteWritable, NullWritable> {

	private final ByteWritable RANKING = new ByteWritable((byte) 0);
	private static final NullWritable NULL = NullWritable.get();

	@Override
	public void map (LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{

		//Tokenize the line in input
		StringTokenizer itr = new StringTokenizer(value.toString());

		if (itr.hasMoreTokens()) {
			itr.nextToken();
		}else{
			return;
		}
		if(itr.hasMoreTokens()){
			String rating = itr.nextToken();
			RANKING.set((byte) Math.round(Double.parseDouble(rating)));
		}else{
			return;
		}
		
		context.write(RANKING, NULL);
	}
}
