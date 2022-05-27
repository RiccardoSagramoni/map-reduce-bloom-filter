package it.unipi.hadoop.tester;

import it.unipi.hadoop.BooleanArrayWritable;
import it.unipi.hadoop.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTester extends Reducer<IntWritable, IntArrayWritable, IntWritable, BooleanArrayWritable> {
}
