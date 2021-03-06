package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {

	public static void main(String[] args) throws Exception {
		String finalOutputDir = "default.out";
		Integer iterations = 1;
		
		try {
			finalOutputDir = args[1];
			iterations = Integer.parseInt(args[2]);
		} catch (Exception e) {}
		
		System.out.println("Iterations: " + iterations.toString());
		
		// Initial clean
		args[1] = finalOutputDir + "0";
		
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "\n\n");
		ToolRunner.run(conf, new PageRankInit(), args);
		conf.set("textinputformat.record.delimiter", "\n");
		
		for (Integer i=0; i<iterations; ) {
			String inputDir = finalOutputDir + i.toString();
			i++;
			String outputDir = finalOutputDir + i.toString();
			
			String[] newArgs = {inputDir, outputDir};
			ToolRunner.run(conf, new PageRank(), newArgs);
		}
		
		// Remove out-links for final output
		args[0] = finalOutputDir + iterations.toString();
		args[1] = finalOutputDir;
		ToolRunner.run(conf, new PageRankFinish(), args);
	}
}
