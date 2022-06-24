from pyspark import SparkContext
import argparse

def main():

	sc = SparkContext(appName="COUNT_NUMBER_OF_KEYS", master="yarn")

if __name__ == '__main__':
	main()
