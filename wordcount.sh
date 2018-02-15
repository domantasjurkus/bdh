export HADOOP_CLASSPATH="$(pwd)/target/bigdata-0.jar" && \
hdfs dfs -rm -r wc && \
hadoop mapreduce.WordCount_v0 wiki.txt wc
