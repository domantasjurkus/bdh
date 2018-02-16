export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r -f hamster[0-9] && \
hdfs dfs -rm -r -f hamster && \
hadoop mapreduce.Main hamster.txt hamster 2
