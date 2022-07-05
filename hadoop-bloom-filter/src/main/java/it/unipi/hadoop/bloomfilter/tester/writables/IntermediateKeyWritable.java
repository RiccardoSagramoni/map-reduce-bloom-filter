package it.unipi.hadoop.bloomfilter.tester.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for the key of the intermediate values between the two mappers and the reducer.
 * <br>
 * This class allows the MapReduce application to sort the intermediate values, so that the first
 * value retrieved through the Iterable structure will be the Bloom Filter
 */
public class IntermediateKeyWritable implements WritableComparable<IntermediateKeyWritable> {

	// Actual key (rating of the movies)
	private Byte rating;

	// Boolean flag for the corresponding value:
	// true if the value is a bloom filter,
	// false if it's the array of indexes to set
	private Boolean isBloomFilter;

	
	public IntermediateKeyWritable() {
		rating = null;
		isBloomFilter = null;
	}

	@Override
	public int compareTo(IntermediateKeyWritable o) {
		if (o.getIsBloomFilter() == null) {
			return 1;
		}
		if (isBloomFilter == null) {
			return -1;
		}

		return -Boolean.compare(isBloomFilter, o.getIsBloomFilter());
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeByte(rating);
		dataOutput.writeBoolean(isBloomFilter);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		rating = dataInput.readByte();
		isBloomFilter = dataInput.readBoolean();
	}

	public void set (byte rating, boolean isBloomFilter) {
		this.rating = rating;
		this.isBloomFilter = isBloomFilter;
	}

	public Byte getRating() {
		return rating;
	}

	public Boolean getIsBloomFilter() {
		return isBloomFilter;
	}

	@Override
	public String toString() {
		return "IntermediateKeyWritable{" +
				"rating=" + rating +
				", isBloomFilter=" + isBloomFilter +
				'}';
	}

}
