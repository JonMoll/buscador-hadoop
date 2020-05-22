#!/bin/sh
hdfs dfs -rm -r /MyWordCount
hadoop jar MyWordCount.jar MyWordCount /ParcialTest /MyWordCount
