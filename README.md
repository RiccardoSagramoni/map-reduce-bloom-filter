# map-reduce-bloom-filter

Project for **Cloud Computing** course at University of Pisa (MSc *Computer Engineering* and *Artificial Intelligence and Data Engineering*).

The aim of this project is to design a MapReduce application to build and test bloom filters (space efficient probabilistic structure used for membership testing) over the movies of the IMDB dataset.

The MapReduce application has to be implemented both with **Hadoop 3.1.3** (using Java 1.8) and **Spark 2.4.4** (using Python 3.6). The configured cluster was composed of four Ubuntu VM, deployed on the cloud infrastructure of the University of Pisa.

Credits to Riccardo Sagramoni, Veronica Torraca, Fabiano Pilia and Emanuele Tinghi.

## Strucure of the GitHub repository
```
.
├── dataset               : IMDB dataset file
├── docs                  : Project specification and report
├── hadoop-bloom-filter   : Java code for the Hadoop application
├── sh-scripts            : Linux script to automatize the execution of the applications
├── spark-bloom-filter    : Python code for the Spark application
└── util                  : Python scripts that sets the dataset up for the applications
```

## Project structure on the Linux Virtual Machines
```
.
├── 0_launch-partition-dataset.sh
├── 1_launch-linecount.sh
├── 2a_launch-hadoop-bloomfilter.sh
├── 2b_launch-hadoop-tester.sh
├── 3a_launch-spark-bloomfilter.sh
├── 3b_launch-spark-tester.sh
├── data
│   └── imdb.tsv
├── hadoop
│   ├── bloom-filter-1.0-SNAPSHOT.jar
│   └── bloom-filter.properties
├── spark
│   ├── bloomfilters_builder.py
│   ├── bloomfilters_tester.py
│   └── bloomfilters_util.py
└── util
    ├── count-number-of-keys.py
    └── split-dataset.py

```

