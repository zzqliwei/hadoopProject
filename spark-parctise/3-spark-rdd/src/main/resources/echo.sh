#!/bin/bash

RESULT=""
while read LINE; do
   RESULT=${RESULT}" "${LINE}
done
echo "Running shell script slave1";
echo ${RESULT} > /home/hadoop-twq/spark-course/out.txt
