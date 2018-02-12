package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount_v2 extends Configured implements Tool {
	static enum Counters { INPUT_WORDS }
	private final static IntWritable one = new IntWritable(2);

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_v2(), args));
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text word = new Text();
		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();
		private long numRecords = 0;
		private String inputFile;

		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
				fis.close();
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file ’" +
						patternsFile + "’ : " + StringUtils.stringifyException(ioe));
			}
		}

		@SuppressWarnings("deprecation")
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
			inputFile = conf.get("map.input.file");

			if (conf.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(conf);
				} catch (IOException ioe) {
					System.err.println("Caught exception while getting cached files: "+StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}
		}

		public void cleanup(Context context) {
			patternsToSkip.clear();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = caseSensitive ? value.toString() : value.toString().toLowerCase();

			for (String pattern : patternsToSkip) {
				line = line.replaceAll(pattern, "");
			}

			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
				context.getCounter(Counters.INPUT_WORDS).increment(1);
			}

			if ((++numRecords % 100) == 0) {
				// This is how you comment
				context.setStatus("Finished processing "+numRecords+" records "+"from the input file: "+inputFile);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value: values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCount-v3");
		job.setJarByClass(WordCount_v2.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		// Final output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i=0; i<args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), job.getConfiguration());
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			} else
				other_args.add(args[i]);
		}

		FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}