#!/bin/bash                                                                                                           

/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-*streaming*.jar \
-file /home/ubuntu/LDSA/A2/tweet_analysis/mapper.py    -mapper /home/ubuntu/LDSA/A2/tweet_analysis/mapper.py \
-file /home/ubuntu/LDSA/A2/tweet_analysis/reducer.py   -reducer /home/ubuntu/LDSA/A2/tweet_analysis/reducer.py \
-input input/tweets/files/* -output output
