#!/bin/bash
if [ $# -ne 2 ]; then
	echo "Usage: $0 <jar file (without .jar)> <θ>"
	exit 1
fi

# check if the .jar exist
if [ ! -e ${1}.jar ]; then
	echo "${1}.jar not exist"
	exit 2
fi

arg1=$1
arg2=$2

hadoop jar ${arg1}.jar ${arg1} ${arg2}
