package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.writables.BooleanArrayWritable;
import it.unipi.hadoop.bloomfilter.writables.GenericObject;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MapperTesterForBloomFilters
        extends Mapper<ByteWritable, BooleanArrayWritable, ByteWritable, GenericObject>
{
    // Logger
    private static final Logger LOGGER = LogManager.getLogger(MapperTesterForBloomFilters.class);

    private final GenericObject object = new GenericObject();

    @Override
    public void map (ByteWritable key, BooleanArrayWritable value, Context context)
            throws IOException, InterruptedException
    {
        object.set(value);
        context.write(key, object);
        // LOGGER.debug("Map bloom filter " + object.get() + " for key " + key);
    }

}
