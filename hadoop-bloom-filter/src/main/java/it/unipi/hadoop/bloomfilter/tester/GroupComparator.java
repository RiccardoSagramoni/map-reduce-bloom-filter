package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.tester.writables.IntermediateKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparator for sorting naturally only the rating key of the composite key
 */
class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(IntermediateKeyWritable.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntermediateKeyWritable keyWritable1 = (IntermediateKeyWritable) w1;
        IntermediateKeyWritable keyWritable2 = (IntermediateKeyWritable) w2;
        return Byte.compare(keyWritable1.getRating(), keyWritable2.getRating());
    }
}
