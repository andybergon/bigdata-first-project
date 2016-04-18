package topproducts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

/**
 * Method 1: First create the JobConf object “job1″ for the first job and set
 * all the parameters with “input” as inputdirectory and “temp” as output
 * directory. Execute this job: JobClient.run(job1). Immediately below it,
 * create the JobConf object “job2″ for the second job and set all the
 * parameters with “temp” as inputdirectory and “output” as output directory.
 * Finally execute second job: JobClient.run(job2). Method 2: Create two JobConf
 * objects and set all the parameters in them just like (1) except that you
 * don’t use JobClient.run. Then create two Job objects with jobconfs as
 * parameters: Job job1=new Job(jobconf1); Job job2=new Job(jobconf2); Using the
 * jobControl object, you specify the job dependencies and then run the jobs:
 * JobControl jbcntrl=new JobControl(“jbcntrl”); jbcntrl.addJob(job1);
 * jbcntrl.addJob(job2); job2.addDependingJob(job1); jbcntrl.run();
 */
public class TopProductsChain extends Configured implements Tool {
	private static final IntWritable ONE = new IntWritable(1);

	/*
	 * Abbiamo righe in questo formato 2015-8-20,pesce,formaggio,insalata,pane
	 * con il primo mapper tagliamo il giorno dalla data e formiamo righe con
	 * data,prodotto come chiave e 1 come valore 2015-8,pesce,1
	 * 2015-8,formaggio,1 quindi in output abbiamo Text, IntWritable
	 */
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text keyDateProduct = new Text();

		protected void map(Text key, LongWritable value, Context ctx) throws IOException, InterruptedException {
			String line = value.toString();
			String[] rowElements = line.split(",");
			String dateFormatted = rowElements[0].substring(0, 7);
			if (dateFormatted.lastIndexOf("-") == 6) {
				dateFormatted = dateFormatted.substring(0, 6);
			}
			for (int i = 1; i < rowElements.length; i++) {
				keyDateProduct.set(dateFormatted + "," + rowElements[i]);
				ctx.write(key, ONE);
			}
		}
	}

	/*
	 * Il primo reducer ci fa le somme quindi 2015-8,pesce,1 2015-8,pesce,1
	 * 2015-8,pesce,2
	 */
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/*
	 * Questo secondo mapper deve aggregare per chiave prendendo solamente la
	 * data e associando la lista prodotto,quantità 2015-8,pesce,2,formaggio,30,
	 * .... NON so quando prendere i 5 più venduti
	 */
	public static class Mapper2 extends Mapper<Text, Text, Text, IntWritable> {

		@Override
		protected void map(Text key, Text value, Context ctx) throws IOException, InterruptedException {
			ctx.write(key, new IntWritable(Integer.valueOf(value.toString())));
		}
	}

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path temp1 = new Path("temp");
		Path output = new Path(args[1]);
		Configuration conf = getConf();

		Job job1 = new Job(conf, "top-prod-pass-1");
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp1);
		job1.setJarByClass(TopProductsChain.class);
		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		boolean succ = job1.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job1 failed, exiting");
			return -1;
		}

		Job job2 = new Job(conf, "top-prod-pass-2");
		FileInputFormat.setInputPaths(job2, temp1);
		FileOutputFormat.setOutputPath(job2, output);
		job2.setJarByClass(TopProductsChain.class);
		job2.setMapperClass(Mapper2.class);
		// job2.setReducerClass(Reducer1.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setNumReduceTasks(1);
		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}

		return 0;

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: TopKRecords /path/to/citation.txt output_dir");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new TopProductsChain(), args);
		System.exit(res);
	}
}