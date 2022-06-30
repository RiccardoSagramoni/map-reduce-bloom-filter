#!/bin/sh

# Declare variable for scripts
TRAIN_DATASET="data/train_imdb.tsv"
LINECOUNT="data/linecount"
OUTPUT_BLOOMFILTERS="hadoop-output/bloomfilters"

FOLDER="hadoop/"
JAR="bloom-filter-1.0-SNAPSHOT.jar"

false_positive_probability="0.2"



# Clean outputs
hadoop fs -rm -r $OUTPUT_BLOOMFILTERS

# Launch builder of bloom filters
cd $FOLDER
hadoop jar $JAR it.unipi.hadoop.bloomfilter.builder.BloomFilter \
$false_positive_probability $TRAIN_DATASET $LINECOUNT $OUTPUT_BLOOMFILTERS
cd -

echo "\nBLOOM FILTER BUILDER COMPLETED\n"
