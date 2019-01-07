Make sure hadoop is running and everything is formatted

1) Make input directory

hadoop dfs -mkdir -p /usr/local/hadoop/input

2) Copy input file to hadoop

hadoop dfs -copyFromLocal "File to copy path" /usr/local/hadoop/input

(remove "")

3) Run jar file command

hadoop jar new.jar composite /usr/local/hadoop/input /usr/local/hadoop/output /usr/local/hadoop/output1 /usr/local/hadoop/output2 /usr/local/hadoop/output3 1

The 1 at end is the number of reducers