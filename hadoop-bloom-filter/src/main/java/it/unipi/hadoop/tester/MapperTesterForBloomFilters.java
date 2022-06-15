package it.unipi.hadoop.tester;

import it.unipi.hadoop.BooleanArrayWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperTesterForBloomFilters
        extends Mapper<ByteWritable, BooleanArrayWritable, ByteWritable, BooleanArrayWritable>
{
    @Override
    public void map (ByteWritable key, BooleanArrayWritable value, Context context)
            throws IOException, InterruptedException
    {
        super.map(key, value, context);
    }
}
