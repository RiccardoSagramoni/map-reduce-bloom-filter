package it.unipi.hadoop.bloomfilter.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MapReduceParameters {

	private static final String PATH = "./bloom-filter.properties";
	private static MapReduceParameters instance = null;

	private final int numberOfReducersBuilder;
	private final int numberOfReducersTester;
	private final int linesPerMapBuilder;
	private final int linesPerMapTester;



	public static MapReduceParameters getInstance() throws IOException {
		if (instance == null) {
			instance = new MapReduceParameters();
		}
		return instance;
	}



	private MapReduceParameters () throws IOException {
		// Load application's properties
		Properties properties = new Properties();

		try (FileInputStream file = new FileInputStream(PATH)) {
			properties.load(file);
		}

		// Retrieve the properties
		numberOfReducersBuilder = Integer.parseInt(properties.getProperty("number_reducers.builder"));
		numberOfReducersTester = Integer.parseInt(properties.getProperty("number_reducers.tester"));
		linesPerMapBuilder = Integer.parseInt(properties.getProperty("lines_per_map.builder"));
		linesPerMapTester = Integer.parseInt(properties.getProperty("lines_per_map.tester"));

		System.out.println(this);
	}



	public int getNumberOfReducersBuilder() {
		return numberOfReducersBuilder;
	}

	public int getNumberOfReducersTester() {
		return numberOfReducersTester;
	}

	public int getLinesPerMapBuilder() {
		return linesPerMapBuilder;
	}

	public int getLinesPerMapTester() {
		return linesPerMapTester;
	}

	@Override
	public String toString() {
		return "MapReduceParameters{" +
				"numberOfReducersBuilder=" + numberOfReducersBuilder +
				", numberOfReducersTester=" + numberOfReducersTester +
				", linesPerMapBuilder=" + linesPerMapBuilder +
				", linesPerMapTester=" + linesPerMapTester +
				'}';
	}
}
