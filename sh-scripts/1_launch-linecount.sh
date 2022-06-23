#!/bin/sh

# Declare variable for scripts
TRAIN_DATASET="data/train_imdb.tsv"
LINECOUNT="data/linecount"
LOCAL_LINECOUNT="/tmp/linecount"


# Clean output
hadoop fs -rm -r $LINECOUNT

# Launch MapReduce job: count how many lines for each rating key
spark-submit --master yarn util/count-number-of-keys.py $TRAIN_DATASET $LINECOUNT

# Merge the result files and upload the merge
hadoop fs -getmerge $LINECOUNT $LOCAL_LINECOUNT
hadoop fs -rm -r $LINECOUNT
hadoop fs -copyFromLocal $LOCAL_LINECOUNT $LINECOUNT
rm $LOCAL_LINECOUNT

echo "\nLinecount completed\n"
