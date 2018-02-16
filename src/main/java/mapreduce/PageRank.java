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
	static String DELIMITER = "&&&";

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		// input = <offset: article score [out_links]>
		public void map(LongWritable __, Text val, Context context) throws IOException, InterruptedException {
			String[] line = val.toString().split("\\s+");
			
			String article = line[0];
			Double score = 0.0;
			try {
				score = Double.parseDouble(line[1]);
			} catch(Exception e) {}
			
			Integer outlinkCount = line.length-2;
			Double contribution = score/outlinkCount;

			String outLinks = "";
		
			// Emit contributions to out-links
			for (int i=0; i<outlinkCount; i++) {
				Text outlink = new Text(line[i+2]);
				context.write(outlink, new Text(contribution.toString()));
				outLinks += outlink + " ";
			}

			// Save out-links, padded with a placeholder score
			context.write(new Text(article), new Text("0.0" + DELIMITER + outLinks));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		// Contribution entry: <article: contribution>
		// Out-link entry: <article: 0.0 DELIMITER [out_links]>
		public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
			Iterator<Text> itr = lines.iterator();
			if (!itr.hasNext()) return;
			
			Double sum = 0.0;
			String outLinks = "";
			String[] line = {};
			
			while (itr.hasNext()) {
				line = itr.next().toString().split(DELIMITER);

				// Save out-links
				if (line.length > 1) {
					outLinks = line[1];
				} else {
					sum += Double.parseDouble(line[0]);
				}
			}
			
			// Save score
			Double score = BASE_SCORE + DAMMING_FACTOR*sum;
			
			// Save out-links if present
			context.write(key, new Text(score.toString() + " " + outLinks));
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
		System.out.println(args[0] + " -> " + args[1]);
		 
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		System.exit(ToolRunner.run(conf, new PageRank(), args));
	}

}