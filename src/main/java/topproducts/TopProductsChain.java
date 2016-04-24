package topproducts;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopProductsChain extends Configured implements Tool {
	private static final IntWritable ONE = new IntWritable(1);

	/* Abbiamo righe in questo formato 2015-8-20,pesce,formaggio,insalata,pane
	con il primo mapper tagliamo il giorno dalla data e formiamo righe con data,prodotto come
	chiave e 1 come valore 

	2015-8-16,pesce,formaggio
	=>
	2015-8,pesce,1
	2015-8,formaggio,1

	quindi in output abbiamo Text, IntWritable*/
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text keyDateProduct = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			String line = value.toString();
			String[] rowElements = line.split(",");
			String dateFormatted = rowElements[0].substring(0, 7);

			for (int i = 1; i < rowElements.length; i++) {
				keyDateProduct.set(dateFormatted + ":" + rowElements[i]);
				ctx.write(keyDateProduct, ONE);
			}
		}
	}
	
	/* Il primo reducer ci fa le somme quindi 
	 * 2015-8:pesce 1  
	 * 2015-8:pesce 1
	 * =>
	 * 2015-8:pesce 2
	 */
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	/* Questo secondo mapper deve splittare la chiave data:prodotto in data: e il valore deve diventare prodotto quantità
	 * 2015-8:pesce 2
	 * 2015-8:formaggio 30
	 * => 
	 * 2015-8: pesce 2
	 * 2015-8: formaggio 30
	 */
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			Text dataKey = new Text();
			Text prodQuantityValue = new Text();
			String data, prodQuantity;

			String[] row = value.toString().split(":");
			data = row[0] + ":";
			prodQuantity = row[1];

			dataKey.set(data);
			prodQuantityValue.set(prodQuantity);

			ctx.write(dataKey, prodQuantityValue);
		}
	}

	/*
	 * key= 2015-8:
	 * value= pesce	2
	 * 2015-8:	pesce	2
	 * 2015-8:	formaggio	30
	 * =>
	 * 2015-8:	pesce 2, formaggio 30 
	 * 
	 * */

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> countMap = new TreeMap<String, Integer>();
			Map<String, Integer> sortedMap = new TreeMap<String, Integer>();
			Text monthlyProducts = new Text();

			for (Text value : values) {
				String line = value.toString();
				String[] lineList = line.split("\t");
				String product = lineList[0];
				String tokenQuantity = lineList[1];
				int quantityInteger = Integer.parseInt(tokenQuantity);
				countMap.put(product, quantityInteger);
			}
			sortedMap = countMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> {
						throw new AssertionError();
					}, LinkedHashMap::new));

			String fiveProducts = "";
			int counter = 0;
			for (String keySorted : sortedMap.keySet()) {
				if (counter == 5) { // TODO: check if 6
					break;
				}
				fiveProducts = fiveProducts + keySorted + " " + sortedMap.get(keySorted).toString();
				if (counter != 4) {
					fiveProducts += ", ";
				}
				counter++;

			}
			monthlyProducts.set(fiveProducts);
			context.write(key, monthlyProducts);
		}
	}
	
	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Path temp = new Path("tmp");

		Configuration conf = getConf();
		boolean succ = false;

		/* JOB 1 */
		Job job1 = new Job(conf, "top-prod-pass-1");

		FileInputFormat.addInputPath(job1, input);
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

		FileInputFormat.setInputPaths(job2, temp);
		FileOutputFormat.setOutputPath(job2, output);
		job2.setJarByClass(TopProductsChain.class);

		job2.setMapperClass(Mapper2.class);
		// job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);

		// job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setInputFormatClass(TextInputFormat.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setNumReduceTasks(1); // ?

		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}

		return 0;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TopProductsChain(), args);
		System.exit(res);
	}
}
