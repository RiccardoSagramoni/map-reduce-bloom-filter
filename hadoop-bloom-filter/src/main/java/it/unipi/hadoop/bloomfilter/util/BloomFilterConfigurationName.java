package it.unipi.hadoop.bloomfilter.util;

public enum BloomFilterConfigurationName {
    NUMBER("bloom.filter.number"),
    RATING_KEY("bloom.filter.size.key"),
    SIZE_VALUE("bloom.filter.size.value"),
    NUMBER_HASH("bloom.filter.hash");

    private final String label;

    BloomFilterConfigurationName(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return label;
    }

}

