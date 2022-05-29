package it.unipi.hadoop.tester;

import it.unipi.hadoop.BooleanArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperBloomFilters extends Mapper<IntWritable, BooleanArrayWritable, IntWritable, BooleanArrayWritable> {

    @Override
    protected void map(IntWritable key, BooleanArrayWritable value, Mapper<IntWritable, BooleanArrayWritable, IntWritable, BooleanArrayWritable>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

}
