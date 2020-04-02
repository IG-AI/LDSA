#!/bin/bash

/usr/local/hadoop/bin/hadoop jar \
/usr/local/hadoop/share/hadoop/mapreduce/hadoop*examples*.jar \
wordcount /home/ubuntu/wordcount/input \
/home/ubuntu/wordcount/output \
