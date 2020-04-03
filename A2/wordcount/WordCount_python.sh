#!/bin/bash

/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-*streaming*.jar \
-file /home/ubuntu/LDSA/A2/wordcount/mapper.py    -mapper /home/ubuntu/LDSA/A2/wordcount/mapper.py \
-file /home/ubuntu/LDSA/A2/wordcount/reducer.py   -reducer /home/ubuntu/LDSA/A2/wordcount/reducer.py \
-input input -output /home/ubuntu/LDSA/A2/wordcount/output/python
