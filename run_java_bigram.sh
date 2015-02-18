#!/bin/bash
if [ $# -ne 1 ]; then
	echo "Usage: $0 <jar file (without .jar)>"
	exit 1
fi

# check if the .jar exist
if [ ! -e ${1}.jar ]; then
	echo "${1}.jar not exist"
	exit 2
fi

arg1=$1

hadoop jar ${arg1}.jar ${arg1}
