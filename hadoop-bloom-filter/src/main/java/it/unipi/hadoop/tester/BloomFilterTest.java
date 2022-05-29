package it.unipi.hadoop.tester;

import it.unipi.hadoop.BooleanArrayWritable;
import it.unipi.hadoop.IntArrayWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BloomFilterTest extends Reducer<Text, IntArrayWritable, Text, DoubleWritable>  {

    //Size of the bloom filter to be read from the configuration
    private int BLOOM_FILTER_SIZE;
    private final DoubleWritable SERIALIZABLE_FALSE_POSITIVE = new DoubleWritable();

    @Override
    public void setup(Context context){
        BLOOM_FILTER_SIZE = context.getConfiguration().getInt("bloom.filter.size", 0);
    }

    @Override
    public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
            throws IOException, InterruptedException{

        //Create the bloomFilter
        BooleanWritable[] bloomFilter = new BooleanWritable[BLOOM_FILTER_SIZE]; // TODO dummy

        double totalTests = 0, falsePositives = 0;

        //Get the intermediate results from the mapper
        for(IntArrayWritable array : values){
            IntWritable[] intArray = (IntWritable[]) array.toArray();

            //Iterate the bloomFilter position in the intArray array
            for(IntWritable i : intArray){
                int index = i.get();

                //Checking for out of bound indexes
                if(index >= BLOOM_FILTER_SIZE){
                    System.err.println("[TEST-REDUCER]: Index out of bound");
                }

                if(!bloomFilter[index].get()){
                    totalTests++;
                    continue;
                }

                if(index == (BLOOM_FILTER_SIZE - 1)){
                    if(bloomFilter[index].get()) {
                        totalTests++;
                        falsePositives++;
                    }
                }
            }
        }

        SERIALIZABLE_FALSE_POSITIVE.set(falsePositives/totalTests);
        context.write(key, SERIALIZABLE_FALSE_POSITIVE);
    }
}
