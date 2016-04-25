package pairsinreceipt;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.DurationFormatter;

public class PairsInReceipt extends Configured implements Tool {

	/*
	 * 2015-01 a,b,c
	 * =>
	 * a,b	1
	 * b,a	1
	 * a,c	1
	 * c,a	1
	 * b,c	1
	 * c,b	1
	 * a	1
	 * b	1
	 * c	1
	 * total	1
	 * 
	 * */
	public static class PIRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		private static final Text TOTAL_RECEIPTS = new Text("total");

		@Override
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

			String[] receiptProducts = value.toString().split(",");

			for (int i = 1; i < receiptProducts.length; i++) {
				for (int j = i + 1; j < receiptProducts.length; j++) {
					String product1 = receiptProducts[i];
					String product2 = receiptProducts[j];

					Text pair1 = new Text(product1 + "," + product2);
					Text pair2 = new Text(product2 + "," + product1);

					ctx.write(pair1, ONE);
					ctx.write(pair2, ONE);
				}
				ctx.write(new Text(receiptProducts[i]), ONE);
			}
			ctx.write(TOTAL_RECEIPTS, ONE);
		}
	}
	
	/*
	 * a,b	1
	 * a,b	1
	 * a,b	1
	 * b,c	1
	 * b,c	1
	 * =>
	 * a,b	3
	 * b,c	2
	 * 
	 * */
	public static class PIRCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context ctx)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			ctx.write(key, new IntWritable(sum));
		}
	}
	
	/* 
	 * 
	 * a,b	3
	 * b,c	2
	 * a	2
	 * b	2
	 * total	3
	 * =>
	 * a,b x%,y%
	 * b,c x%,y% 
	 * 
	 */
	public static class PIRReducer extends Reducer<Text, IntWritable, Text, Text> {
		private Map<String, Integer> productPair2receiptsNumber = new HashMap<String, Integer>();
		private Map<String, Integer> product2receiptsNumber = new HashMap<String, Integer>();
		private IntWritable totalReceipts;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context ctx)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			IntWritable receiptsQuantity = new IntWritable(sum);

			if (key.toString().equals("total")) {
				this.totalReceipts = receiptsQuantity;
			} else if (key.toString().contains(",")) {
				this.productPair2receiptsNumber.put(key.toString(), receiptsQuantity.get());
			} else {
				this.product2receiptsNumber.put(key.toString(), receiptsQuantity.get());
			}
		}

		@Override
		protected void cleanup(Context ctx) throws IOException, InterruptedException {
			for (String productsPair : this.productPair2receiptsNumber.keySet()) {

				String[] productsInPair = productsPair.split(",");
				String product1 = new String(productsInPair[0]);

				double productsPairQuantity = this.productPair2receiptsNumber.get(productsPair).intValue();
				double product1Quantity = this.product2receiptsNumber.get(product1).intValue();
				int totalQuantity = this.totalReceipts.get();

				// supporto della regola di associazione p1→p2
				Double support = productsPairQuantity / totalQuantity;
				// confidenza della regola di associazione p1→p2
				Double confidence = productsPairQuantity / product1Quantity;

				DecimalFormat decimalFormat = new DecimalFormat("#0.0%");
				String supportPercentage = decimalFormat.format(support);
				String confidencePercentage = decimalFormat.format(confidence);

				String percentages = supportPercentage + ", " + confidencePercentage;

				Text key = new Text(productsPair);
				Text value = new Text(percentages);

				ctx.write(key, value);
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		boolean succ = false;

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		Job job = new Job(getConf());
		job.setJobName(PairsInReceipt.class.getSimpleName());
		job.setJarByClass(PairsInReceipt.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(PIRMapper.class);
		job.setCombinerClass(PIRCombiner.class);
		job.setReducerClass(PIRReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		long startTime = System.currentTimeMillis();

		succ = job.waitForCompletion(true);

		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;

		String formattedElapsedTime = DurationFormatter.formatDuration(elapsedTime);

		if (!succ) {
			System.out.println("Job FAILED after " + formattedElapsedTime);
			return -1;
		} else {
			System.out.println("Job COMPLETED in " + formattedElapsedTime);
			return 0;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PairsInReceipt(), args);
		System.exit(res);
	}

}
