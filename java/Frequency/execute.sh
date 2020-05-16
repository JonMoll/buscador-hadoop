#!/bin/sh
hdfs dfs -rm -r /Frequency
hadoop jar Frequency.jar Frequency /Documents /Frequency
