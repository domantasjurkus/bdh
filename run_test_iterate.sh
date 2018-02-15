export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r -f hamster && \
hdfs dfs -rm -r -f hamster[1-9] && \
hadoop mapreduce.PageRank hamster0 hamster 3
