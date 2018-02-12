# This thing should run the whole process from start to finish
export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r pagerank && \
hadoop mapreduce.PageRank wiki.txt pagerank
