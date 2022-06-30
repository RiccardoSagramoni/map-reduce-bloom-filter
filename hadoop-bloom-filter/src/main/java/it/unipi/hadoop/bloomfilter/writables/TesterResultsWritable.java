package it.unipi.hadoop.bloomfilter.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TesterResultsWritable implements Writable {

	private int falsePositiveOccurrences;
	private int totalOccurrences;
	private double falsePositivePercentage;



	public TesterResultsWritable () {
		set(0, 0, Double.NaN);
	}

	public TesterResultsWritable (int falsePositiveOccurrences, int totalOccurrences) {
		set(falsePositiveOccurrences, totalOccurrences);
	}

	public int getFalsePositiveOccurrences () {
		return falsePositiveOccurrences;
	}

	public int getTotalOccurrences () {
		return totalOccurrences;
	}

	public double getFalsePositivePercentage () {
		return falsePositivePercentage;
	}

	@Override
	public String toString () {
		return "(" + falsePositiveOccurrences + ", "
				+ totalOccurrences + ", "
				+ falsePositivePercentage + ")";
	}



	/**
	 * Set the values of the class
	 *
	 * @param falsePositiveOccurrences number of false positives
	 * @param totalOccurrences amount of all the test samples
	 * @param falsePositivePercentage percentage of false positives
	 */
	private void set (int falsePositiveOccurrences,
	                  int totalOccurrences,
	                  double falsePositivePercentage)
	{
		this.falsePositiveOccurrences = falsePositiveOccurrences;
		this.totalOccurrences = totalOccurrences;
		this.falsePositivePercentage = falsePositivePercentage;
	}


	/**
	 * Set the values of the class (automatically compute the false positive percentage)
	 *
	 * @param falsePositiveOccurrences number of false positives
	 * @param totalOccurrences amount of all the test samples
	 */
	public void set (int falsePositiveOccurrences, int totalOccurrences) {
		set(
				falsePositiveOccurrences,
				totalOccurrences,
				(double)falsePositiveOccurrences / (double)totalOccurrences
		);
	}


	/**
	 * Serialize the fields of this object to dataOutput.
	 *
	 * @param dataOutput DataOutput to serialize this object into
	 * @throws IOException IOException
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(falsePositiveOccurrences);
		dataOutput.writeInt(totalOccurrences);
		dataOutput.writeDouble(falsePositivePercentage);
	}


	/**
	 * Deserialize the fields of this object from dataInput.
	 *
	 * @param dataInput DataInput to deserialize this object from.
	 * @throws IOException IOException
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		falsePositiveOccurrences = dataInput.readInt();
		totalOccurrences = dataInput.readInt();
		falsePositivePercentage = dataInput.readDouble();
	}

}
