export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r -f hamster2 && \
hadoop mapreduce.PageRank hamster1 hamster2
