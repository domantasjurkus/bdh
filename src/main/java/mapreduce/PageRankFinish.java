package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankFinish extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		// val: <article score [out-links]>
		public void map(LongWritable __, Text val, Context context) throws IOException, InterruptedException {
			String[] line = val.toString().split("\\s+");
			context.write(new Text(line[0]), new Text(line[1]));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text v : values) {
				context.write(key, new Text(v));
			}
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "PageRankFinish");
		job.setJarByClass(PageRankFinish.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRankFinish(), args));
	}

}