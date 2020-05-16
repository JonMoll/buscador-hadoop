#!/bin/sh
javac Frequency.java -cp $(hadoop classpath)
jar -cvf Frequency.jar Frequency*.class
