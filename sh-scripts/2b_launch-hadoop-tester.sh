#!/bin/sh

# Declare variable for scripts
TEST_DATASET="data/test_imdb.tsv"
LINECOUNT="data/linecount"
OUTPUT_BLOOMFILTERS="hadoop-output/bloomfilters"
OUTPUT_TEST="hadoop-output/test"

FOLDER="hadoop/"
JAR="bloom-filter-1.0-SNAPSHOT.jar"

false_positive_probability="0.2"



# Clean outputs
hadoop fs -rm -r $OUTPUT_TEST

# Launch tester for bloom filters
cd $FOLDER
hadoop jar $JAR it.unipi.hadoop.bloomfilter.tester.BloomFilterTester \
$false_positive_probability $TEST_DATASET $OUTPUT_BLOOMFILTERS $LINECOUNT $OUTPUT_TEST
cd -

echo ""

hadoop fs -cat $OUTPUT_TEST/part*

echo "\nBLOOM FILTER TESTER COMPLETED\n"
