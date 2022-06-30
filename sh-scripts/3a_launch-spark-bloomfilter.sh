#!/bin/sh

# Declare variable for scripts
TRAIN_DATASET="data/train_imdb.tsv"
LINECOUNT="data/linecount"
OUTPUT_BLOOMFILTERS="spark-output/bloomfilters"

FOLDER="spark/"
PYFILE="bloomfilters_builder.py"

false_positive_probability="0.2"



# Clean outputs
hadoop fs -rm -r $OUTPUT_BLOOMFILTERS

# Launch builder of bloom filters
cd $FOLDER
spark-submit --master yarn $PYFILE \
$false_positive_probability $TRAIN_DATASET $LINECOUNT $OUTPUT_BLOOMFILTERS
cd -

echo "\nBLOOM FILTER BUILDER COMPLETED\n"
