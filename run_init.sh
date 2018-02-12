export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r pagerank_00 && \
hadoop mapreduce.PageRankInit wiki.txt pagerank_00