package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

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

public class PageRankInitTokens extends Configured implements Tool {
	static enum Counters { INPUT_WORDS }
	static float DAMMING_FACTOR = 0.85f;

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private boolean caseSensitive = true;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
		}

		public void map(LongWritable __, Text val, Context context) throws IOException, InterruptedException {
			String entry = caseSensitive ? val.toString() : val.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(entry);
			
			Integer counter = 0;
			String token = "";
			String key = "";
			String value = "1.0";
			
			// Get article name
			try {
				if (tokenizer.nextToken().equals("REVISION")) {
					tokenizer.nextToken();
					tokenizer.nextToken();
					key = tokenizer.nextToken();
				}
			} catch (Exception e) {
				return;
			}
			
			// Skip until MAIN
			while (!token.equals("MAIN")) {
				try {
					token = tokenizer.nextToken();
				} catch (Exception e) {
					context.write(new Text("ERROR"), new Text("1"));
				}
			}
			token = tokenizer.nextToken();
			//value += " " + tokenizer.nextToken();
			//value += " " + token;
			
			while (!token.equals("TALK")) {
				try {
					if (counter > 3) break;
					value += " " + token;
					counter++;
					token = tokenizer.nextToken();
					//break;
				} catch (Exception e) {
					context.write(new Text("ERROR"), new Text("2"));
				}
			}
			
			//value += " " + counter.toString();
			
			context.write(new Text(key), new Text(value));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
			Integer sum = 0;
			//Set<String> outLinkSet = new HashSet<String>();
			
			/*if (key.equals("ERROR")) {
				sum += Integer.parseInt(values.toString());
			}
			
			if (key.equals("ERROR")) {
				context.write(key, new Text(sum.toString()));
				return;
			}*/
			
			/*for (Text v : values) {
				outLinkSet.add(v.toString());
			}*/
			//String outLinkString = Arrays.toString(outLinkSet.toArray(new String[outLinkSet.size()])); 
			//Text outValue = new Text(outLinkString);
			
			//context.write(key, values.iterator().next());
			context.write(key, values);
		}
		
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCount-v3");
		job.setJarByClass(PageRankInitTokens.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
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
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(Text.DEFAULT_MAX_LEN);
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "\n\n");
		System.exit(ToolRunner.run(conf, new PageRankInitTokens(), args));
	}

}