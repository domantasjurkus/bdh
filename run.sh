# This thing should run the whole process from start to finish
export HADOOP_CLASSPATH="$(pwd)/target/pagerank-0.jar" && \
hdfs dfs -rm -r -f pagerank_* && \
hadoop mapreduce.Main wiki.txt pagerank 3
