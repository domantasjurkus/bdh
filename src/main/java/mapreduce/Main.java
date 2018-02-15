package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {

	public static void main(String[] args) throws Exception {
		String finalOutputDir = "default.out";
		int iterations = 1;
		
		try {
			finalOutputDir = args[1];
			iterations = Integer.parseInt(args[2]);
		} catch (Exception e) {}
		
		// Initial clean
		args[1] = finalOutputDir + "0";
		
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "\n\n");
		ToolRunner.run(conf, new PageRankInit(), args);
		
		// First iteration
		//String newArgs[] = {"pagerank_00", "pagerank_01"};
		//ToolRunner.run(conf, new PageRank(), newArgs);
		
		for (Integer i=0; i<iterations; ) {
			String inputDir = finalOutputDir + i.toString();
			String outputDir = finalOutputDir + (++i).toString();
			
			String[] newArgs = {inputDir, outputDir};
			ToolRunner.run(conf, new PageRank(), newArgs);
		}
	}
}
