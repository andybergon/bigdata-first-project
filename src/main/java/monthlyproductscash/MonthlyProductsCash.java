package monthlyproductscash;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import topproducts.TopProductsChain;
import topproducts.TopProductsChain.Mapper1;
import topproducts.TopProductsChain.Mapper2;
import topproducts.TopProductsChain.Reducer1;
import topproducts.TopProductsChain.Reducer2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.swing.RowFilter.Entry;
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

public class MonthlyProductsCash extends Configured implements Tool {
	private static final IntWritable ONE = new IntWritable(1);

	/*
	 * Abbiamo righe in questo formato 2015-8-20,pesce,formaggio,insalata,pane
	 * con il primo mapper tagliamo il giorno dalla data e formiamo righe con
	 * data,prodotto come chiave e 1 come valore
	 * 
	 * 2015-8-16,pesce,formaggio => 2015-8,pesce,1 2015-8,formaggio,1
	 * 
	 * quindi in output abbiamo Text, IntWritable
	 */
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text keyDateProduct = new Text();

		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			String line = value.toString();
			String[] rowElements = line.split(",");
			String dateFormatted = rowElements[0].substring(0, 7);
			if (dateFormatted.lastIndexOf("-") == 6) {
				dateFormatted = dateFormatted.substring(0, 6);
			}
			for (int i = 1; i < rowElements.length; i++) {
				keyDateProduct.set(dateFormatted + ":" + rowElements[i]);
				ctx.write(keyDateProduct, ONE);
			}
		}
	}
	/* Il primo reducer ci fa le somme quindi chiave= 2015-8:pesce - valore=2
	 * 2015-8:pesce 1  
	 * 2015-8:pesce 1
	 * =>
	 * 2015-8:pesce 2
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
	 * Questo mapper deve scrivere chiave= pesce - valore= 2015-8:2
	 * pesce	2015-8:2
	 */
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			Text ctxKey = new Text();
			Text ctxValue = new Text();
			String data, prodQuantity, product, quantity;

			String[] row = value.toString().split(":");
			data = row[0] + ":";
			prodQuantity = row[1];
			String[] rowprodQuantity = prodQuantity.split("\t");
			product=rowprodQuantity[0];
			quantity=rowprodQuantity[1];

			ctxKey.set(product);
			ctxValue.set(data+quantity);

			ctx.write(ctxKey, ctxValue);
		}
	}

	/*
	 * Questo mapper deve prendere i prezzi dei prodotti chiave= pesce valore= 3
	 * pesce,3
	 * =>chiave= pesce valore= 3
	 */
	public static class MapperPrice extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			Text ctxKey = new Text();
			Text ctxValue = new Text();
			String data, prodQuantity, product, quantity;

			/*String[] row = value.toString().split(",");
			data = row[0] + ":";
			prodQuantity = row[1];*/
			String[] rowprodQuantity = value.toString().split(",");
			product=rowprodQuantity[0];
			quantity=rowprodQuantity[1];

			ctxKey.set(product);
			ctxValue.set(quantity);

			ctx.write(ctxKey, ctxValue);
		}
	}

	/*
	 *Questo prende i due mapper  pesce	2015-8:2 e pesce,3
	 *deve restituire per la chiave pesce la moltiplicazione tra quantità 2 e prezzo 1
	 *pesce	2015-8:6
	 */
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text monthlyProducts = new Text();
			int i=0;
			String prova="";
			for (Text value : values) {
					/*String[] row = value.toString().split(":");
					String data=row[1];
					int quantity = Integer.parseInt(row[2]);
					int price = Integer.parseInt(row[0]);
					int tot=quantity*price;
				prova=prova+data+":"+tot+" - ";*/
				prova=prova+value.toString()+"--";
			}
			monthlyProducts.set(prova);
			context.write(key, monthlyProducts);
		}
	}

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path p3=new Path(args[1]);
		Path output = new Path(args[2]);
		Path temp = new Path("tmp/tmp");

		Configuration conf = new Configuration();
		boolean succ = false;
		/* JOB 1 */
		Job job1 = new Job(conf, "prod-pass-1");

		FileInputFormat.addInputPath(job1, input);
		//		FileOutputFormat.setOutputPath(job1, output);
		FileOutputFormat.setOutputPath(job1, temp);

		job1.setJarByClass(TopProductsChain.class);

		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		succ = job1.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job1 failed, exiting");
			return -1;
		}

		/* JOB 2 */		
		Job job2 = new Job(conf, "top-prod-pass-2");

		//FileInputFormat.setInputPaths(job2, temp);
		MultipleInputs.addInputPath(job2, temp, TextInputFormat.class, Mapper2.class);
		MultipleInputs.addInputPath(job2, p3, TextInputFormat.class, MapperPrice.class);
		FileOutputFormat.setOutputPath(job2, output);
		job2.setJarByClass(TopProductsChain.class);

		//job2.setMapperClass(Mapper2.class);
		//job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);

		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		//job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setNumReduceTasks(1); //?

		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}
		/*
		 */
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int ecode = ToolRunner.run(new MonthlyProductsCash(), args);
		System.exit(ecode);

	}

}
