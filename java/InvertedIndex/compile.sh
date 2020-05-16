#!/bin/sh
javac InvertedIndex.java -cp $(hadoop classpath)
jar -cvf InvertedIndex.jar InvertedIndex*.class
