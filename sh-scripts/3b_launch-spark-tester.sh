#!/bin/sh

# Declare variable for scripts
TEST_DATASET="data/test_imdb.tsv"
LINECOUNT="data/linecount"
OUTPUT_BLOOMFILTERS="spark-output/bloomfilters"
OUTPUT_TEST="spark-output/test"

FOLDER="spark/"
PYFILE="bloomfilters_tester.py"

false_positive_probability="0.2"



# Clean outputs
hadoop fs -rm -r $OUTPUT_TEST

# Launch tester for bloom filters
cd $FOLDER
spark-submit --master yarn $PYFILE \
$false_positive_probability $TEST_DATASET $OUTPUT_BLOOMFILTERS $LINECOUNT $OUTPUT_TEST
cd -

echo ""

hadoop fs -cat $OUTPUT_TEST/part*

echo "\nBLOOM FILTER TESTER COMPLETED\n"
