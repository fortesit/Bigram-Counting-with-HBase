#!/bin/bash
if [ $# != "1" ]; then
	echo "Usage: $0 <java code (without .java)>"
	exit 1
fi

# check if the java program exist
if [ ! -e ${1}.java ]; then
    echo "File: ${1}.java not exist"
    exit 1
fi
# check if the folder exist or not
if [ ! -e $1 ]; then
	mkdir  $1
fi
javac -classpath `hbase classpath` ${1}.java -d $1
jar -cvf ${1}.jar -C $1 .
