#!/bin/sh

# Declare variable for scripts
FILENAME="imdb.tsv"

hadoop fs -rm -r data/train_$FILENAME
hadoop fs -rm -r data/test_$FILENAME

# Launch MapReduce job: divide the dataset
spark-submit --master yarn util/split-dataset.py $FILENAME

echo "\nDATABASE PARTITION COMPLETED\n"

