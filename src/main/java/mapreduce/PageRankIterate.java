package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
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

public class PageRankIterate extends Configured implements Tool {
	static Integer iterations = 1;

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable __, Text val, Context context) throws IOException, InterruptedException {
			// ...
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// ...
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "PageRank");
		job.setJarByClass(PageRankIterate.class);

		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		// Map output (auto sets reducer input classes)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Final output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i=0; i<args.length; ++i) {
			other_args.add(args[i]);
		}
		FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
		
		// Set iteration count
		try {
			iterations = Integer.parseInt(other_args.get(2));
		} catch (Exception e) { }
		 
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("textinputformat.record.delimiter", "\n\n");
		System.exit(ToolRunner.run(conf, new PageRankIterate(), args));
	}

}