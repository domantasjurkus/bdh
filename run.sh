export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r -f pagerank[0-9] && \
hdfs dfs -rm -r -f pagerank && \
hadoop mapreduce.Main wiki.txt pagerank $1
