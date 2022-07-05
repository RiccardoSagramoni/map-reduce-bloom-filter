package it.unipi.hadoop.bloomfilter.tester;

import it.unipi.hadoop.bloomfilter.tester.writables.IntermediateKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparator for the Secondary Sort of the key in order to sort the composite key (rating, isBloomFilter):
 * <ul>
 *     <li>natural order for the rating field</li>
 *     <li>reverse order for the isBloomFilter field</li>
 * </ul>
 * The second part of the key is a boolean variable indicating whether it is the bloom filter (true) or the array of
 * indexes to set (false).
 */
class KeyComparator extends WritableComparator {

    protected KeyComparator() {
        super(IntermediateKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntermediateKeyWritable keyWritable1 = (IntermediateKeyWritable) w1;
        IntermediateKeyWritable keyWritable2 = (IntermediateKeyWritable) w2;

        int cmp = Byte.compare(keyWritable1.getRating(), keyWritable2.getRating());
        if (cmp != 0) {
            return cmp;
        }

        return -Boolean.compare(keyWritable1.getIsBloomFilter(), keyWritable2.getIsBloomFilter());  // reverse order
    }

}
