#!/bin/sh

# Check for all the stuff we need to function.
for f in gcc javac mvn; do
	which $f > /dev/null 2>&1
	if [ $? -ne 0 ]; then
		echo "Required program $f is missing. Please install or fix your path and try again."
		exit 1
	fi
done

echo "Building TPC-DS Data Generator"

unamestr=`uname`
if [[ "$unamestr" == 'Darwin' ]]; then
  export MYOS="Darwin"
fi

make
echo "TPC-DS Data Generator built, you can now use tpcds-setup.sh to generate data."