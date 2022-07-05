package it.unipi.hadoop.bloomfilter.util;

/**
 * Enumerator type containing the "hardcoded" name for the configuration parameters,
 * in order to avoid to use directly the parameter name inside the code
 */
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

