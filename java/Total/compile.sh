#!/bin/sh
javac Total.java -cp $(hadoop classpath)
jar -cvf Total.jar Total*.class
