#!/bin/sh
javac MyWordCount.java -cp $(hadoop classpath)
jar -cvf MyWordCount.jar MyWordCount*.class
