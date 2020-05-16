#!/bin/sh
hdfs dfs -rm -r /InvertedIndex
hadoop jar InvertedIndex.jar InvertedIndex /Documents /InvertedIndex
