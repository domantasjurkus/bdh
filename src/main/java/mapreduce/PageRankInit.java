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

public class PageRankInit extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable __, Text val, Context context) throws IOException, InterruptedException {
			String[] strings = val.toString().split("\\s+");
			String key = "";
			String revisionNumber = "0";
			String outLinks = "";
			String previousOutlink = "";
			
			boolean outLinksStarted = false;
			int i = 0;
			
			try {
				while (true) {
					// Get title and revision number
					if (strings[i].equals("REVISION")) { 
						key = strings[i+3];
						revisionNumber = strings[i+2];
					}
					
					// Get out-links
					if (outLinksStarted) {
						// Skip duplicates (assuming they come in order)
						if (!strings[i].equals(previousOutlink)) {
							outLinks += " " + strings[i];
							previousOutlink = strings[i];
						}
					}
					
					if (strings[i].equals("MAIN")) {
						outLinksStarted = true;
					}
					
					i++;
					
					if (strings[i].equals("TALK")) {
						break;
					}
				}
			} catch (Exception e) {}
			
			context.write(new Text(key), new Text(revisionNumber + " " + outLinks));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String payload = "";
			Integer lastHighestRevisionNumber = 0;
			
			// Keep only the most recent revision
			for (Text v : values) {
				String line = v.toString();
				String revNo = line.split(" ")[0];
				if (Integer.parseInt(revNo) > lastHighestRevisionNumber) {
					int firstSpaceIndex = revNo.length();
					payload = line.substring(firstSpaceIndex+1, line.length());
				}
			}
			
			context.write(key, new Text("1.0 " + payload));
		}
	}
	
	public void setup(Job job) {
		job.setJarByClass(PageRankInit.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		// Map output (auto sets reducer input classes)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Final output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "PageRankInit");
		this.setup(job);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "\n\n");
		System.exit(ToolRunner.run(conf, new PageRankInit(), args));
	}

}