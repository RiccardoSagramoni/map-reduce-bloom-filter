package it.unipi.hadoop.tester;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperDataset extends Mapper<LongWritable, Text, Text, Text> {
}
