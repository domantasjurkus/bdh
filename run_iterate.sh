export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r -f pagerank_01 && \
hadoop mapreduce.PageRank pagerank_00 pagerank_01
