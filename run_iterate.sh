export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r -f pagerank[1-9] && \
hadoop mapreduce.PageRank pagerank0 pagerank
