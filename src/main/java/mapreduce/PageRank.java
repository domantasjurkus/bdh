package mapreduce;

import java.io.IOException;
import java.util.Iterator;

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

public class PageRank extends Configured implements Tool {
	static Double BASE_SCORE = 0.15;
	static Double DAMMING_FACTOR = 0.85;

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable __, Text val, Context context) throws IOException, InterruptedException {
			String[] line = val.toString().split("\\s+");
			if (line.length < 2) return;
			
			//String key = line[0];
			
			Double score = Double.parseDouble(line[1]);
			Integer outlinkCount = line.length-2;
			Double contribution = score/outlinkCount;
			
			for (int i=0; i<outlinkCount; i++) {
				Text outlink = new Text(line[i+2]);
				context.write(outlink, new Text(contribution.toString()));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> contributions, Context context) throws IOException, InterruptedException {
			Iterator<Text> itr = contributions.iterator();
			if (!itr.hasNext()) return;
			
			Double sum = 0.0;
			
			while (itr.hasNext()) {
				sum += Double.parseDouble(itr.next().toString());
			}
			
			Double score = BASE_SCORE + DAMMING_FACTOR*sum;
			context.write(key, new Text(score.toString()));
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "PageRank");
		job.setJarByClass(PageRank.class);

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
		Configuration conf = new Configuration();
		System.exit(ToolRunner.run(conf, new PageRank(), args));
	}

}